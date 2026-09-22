package blobcachecmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"blobcache.io/blobcache/src/schema/bcfuse"
	"blobcache.io/blobcache/src/schema/bcfuse/scheme_glfs"
	"blobcache.io/blobcache/src/schema/bcns"
	"blobcache.io/blobcache/src/schema/jsonns"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/star"
)

// shutdownTimeout bounds the final buffered-write commit attempted at unmount.
const shutdownTimeout = 30 * time.Second

var fuseMountCmd = star.Command{
	Metadata: star.Metadata{
		Short: "Mount a blobcache volume as a FUSE filesystem",
	},
	Flags: map[string]star.Flag{},
	Pos: []star.Positional{
		volumeNameParam,
		mountpointParam,
	},
	F: func(c star.Context) error {
		svc, err := openService(c)
		if err != nil {
			return err
		}
		volName := volumeNameParam.Load(c)
		nsc := bcns.NewClient(svc, blobcache.OID{})
		nsc.SetDefaultSchema(jsonns.Schema{})
		volh, err := nsc.Open(c.Context, volName, blobcache.Action_ALL)
		if err != nil {
			return err
		}
		// The buffer database is persistent and keyed by volume, so buffered
		// batches survive process restarts and are recovered at Init.
		db, err := openFuseBufferDB(volh.OID)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		fsx := bcfuse.New(db, svc, volh, scheme_glfs.NewScheme())
		if err := fsx.Init(c.Context); err != nil {
			return err
		}
		fuseSrv, err := fs.Mount(mountpointParam.Load(c), fsx.FUSERoot(), &fs.Options{
			MountOptions: fuse.MountOptions{
				Debug: true,
			},
		})
		if err != nil {
			return err
		}
		fuseSrv.Serve()

		// Serve returns on unmount: make one bounded final commit of accepted
		// writes. A failure is returned to the caller rather than dropping the
		// data, which remains in the buffer for the next mount.
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return fsx.Shutdown(ctx)
	},
}

func openFuseBufferDB(oid blobcache.OID) (*sqlx.DB, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return nil, fmt.Errorf("locating cache directory for fuse buffer: %w", err)
	}
	dir := filepath.Join(cacheDir, "blobcache", "bcfuse")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating fuse buffer directory: %w", err)
	}
	dbPath := filepath.Join(dir, hex.EncodeToString(oid[:])+".db")
	db, err := sqlutil.OpenDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening fuse buffer database %s: %w", dbPath, err)
	}
	return db, nil
}

var mountpointParam = &star.Required[string]{
	PosName:  "mountpoint",
	ShortDoc: "the path in the host filesystem to mount the FUSE filesystem",
	Parse:    star.ParseString,
}
