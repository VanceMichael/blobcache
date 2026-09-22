package localvol

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/schemareg"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func mustRefCount(t testing.TB, db *pebble.DB, cid blobcache.CID) uint32 {
	t.Helper()
	k := pdb.TKey{TableID: dbtab.TID_BLOB_REF_COUNT, Key: cid[:16]}.Marshal(nil)
	v, closer, err := db.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0
	}
	require.NoError(t, err)
	defer closer.Close()
	require.Len(t, v, 4)
	return binary.BigEndian.Uint32(v)
}

func newTestLocalSystem(t testing.TB) (*System, *pebble.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(filepath.Join(dir, "pebble"), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "blob"), 0o755))
	blobDir, err := os.OpenRoot(filepath.Join(dir, "blob"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = blobDir.Close() })

	txSys := pdb.NewTxSys(db, dbtab.TID_SYS_TXNS)
	sys := New(Config{NoSync: true}, Env{
		DB:       db,
		BlobDir:  blobDir,
		TxSys:    &txSys,
		MkSchema: schemareg.Factory,
	})
	return &sys, db
}

func testVolume(t testing.TB, sys *System, id ID) *Volume {
	return sys.UpNoErr(Params{
		Key: id,
		Params: blobcache.VolumeConfig{
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 20,
		},
	})
}

func countTableRows(t testing.TB, db *pebble.DB, tid pdb.TableID) int {
	t.Helper()
	sn := db.NewSnapshot()
	defer sn.Close()
	iter, err := sn.NewIter(&pebble.IterOptions{
		LowerBound: pdb.TableLowerBound(tid),
		UpperBound: pdb.TableUpperBound(tid),
	})
	require.NoError(t, err)
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

func cellValue(t testing.TB, db *pebble.DB, volID ID, mvid pdb.MVTag) ([]byte, error) {
	t.Helper()
	k := pdb.MVKey{TableID: dbtab.TID_LOCAL_VOLUME_CELLS, Key: volID.Marshal(nil), Version: mvid}.Marshal(nil)
	v, closer, err := db.Get(k)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return v, nil
}

// TestAbortMutReleasesEverything verifies that abortMut revokes the active
// records, removes the transaction's MVCC rows and releases the per-volume
// write lock so another mutating transaction can start immediately.
func TestAbortMutReleasesEverything(t *testing.T) {
	ctx := context.Background()
	sys, db := newTestLocalSystem(t)
	vol := testVolume(t, sys, 1)

	txb, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	tx := txb.(*localTxnMut)
	require.NoError(t, tx.Save(ctx, []byte("cell data")))
	cid, err := tx.Post(ctx, []byte("blob data"), blobcache.PostOpts{})
	require.NoError(t, err)

	require.NoError(t, sys.abortMut(1, tx.mvid))

	// SYS_TXNS rows: only the sequence counter (key 0) may remain.
	require.Equal(t, 1, countTableRows(t, db, dbtab.TID_SYS_TXNS))
	require.Equal(t, 0, countTableRows(t, db, dbtab.TID_LOCAL_VOLUME_TXNS))
	require.Equal(t, 0, countTableRows(t, db, dbtab.TID_LOCAL_VOLUME_CELLS))
	require.Equal(t, 0, countTableRows(t, db, dbtab.TID_LOCAL_VOLUME_BLOBS))
	require.Equal(t, uint32(0), mustRefCount(t, db, cid))

	// The write lock is released: another mutating transaction starts and works.
	txb2, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	tx2 := txb2.(*localTxnMut)
	require.NoError(t, tx2.Save(ctx, []byte("second")))
	require.NoError(t, tx2.Commit(ctx))

	// abortMut is idempotent on a finished transaction and must not undo
	// committed state.
	require.NoError(t, sys.abortMut(1, tx.mvid))
	v, err := cellValue(t, db, 1, tx2.mvid)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), v)
}

// TestAbortMutRetryAfterPhaseOne simulates an abort whose first phase
// committed (failed marker + record revocation durable, lock released) but
// whose MVCC cleanup did not run. The retry must skip phase one (never
// releasing the lock twice) and finish cleanup without disturbing a newer
// transaction holding the per-volume lock.
func TestAbortMutRetryAfterPhaseOne(t *testing.T) {
	ctx := context.Background()
	sys, db := newTestLocalSystem(t)
	vol := testVolume(t, sys, 1)

	txb, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	tx1 := txb.(*localTxnMut)
	require.NoError(t, tx1.Save(ctx, []byte("old cell")))
	cid, err := tx1.Post(ctx, []byte("old blob"), blobcache.PostOpts{})
	require.NoError(t, err)

	// Simulate a crash after phase 1: failed marker and record revocation
	// are durable, and the write lock has been released.
	ba := db.NewIndexedBatch()
	require.NoError(t, deleteLocalVolumeTxn(ba, 1, tx1.mvid))
	require.NoError(t, sys.txSys.Failure(ba, tx1.mvid))
	require.NoError(t, ba.Commit(nil))
	require.NoError(t, ba.Close())
	sys.mutVol.Unlock(1)

	// A newer transaction acquires the lock and commits.
	txb2, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	tx2 := txb2.(*localTxnMut)
	require.NoError(t, tx2.Save(ctx, []byte("new cell")))
	require.NoError(t, tx2.Commit(ctx))

	// Retry the old abort. It must not touch tx2's lock or rows.
	require.NoError(t, sys.abortMut(1, tx1.mvid))
	require.Equal(t, 0, countTableRows(t, db, dbtab.TID_LOCAL_VOLUME_TXNS))
	require.Equal(t, uint32(0), mustRefCount(t, db, cid))
	v, err := cellValue(t, db, 1, tx2.mvid)
	require.NoError(t, err)
	require.Equal(t, []byte("new cell"), v)

	// Another transaction can still begin, proving the lock was not stolen.
	txb3, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	var dst []byte
	require.NoError(t, txb3.Load(ctx, &dst))
	require.Equal(t, []byte("new cell"), dst)
	require.NoError(t, txb3.Abort(ctx))
}

// TestAbortCommittedIsANoOp verifies that aborting a transaction which
// already committed neither undoes committed rows nor unlocks another
// transaction's write lock.
func TestAbortCommittedIsANoOp(t *testing.T) {
	ctx := context.Background()
	sys, db := newTestLocalSystem(t)
	vol := testVolume(t, sys, 1)

	txb, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	tx := txb.(*localTxnMut)
	require.NoError(t, tx.Save(ctx, []byte("committed cell")))
	require.NoError(t, tx.Commit(ctx))

	// abort after commit must find no active row and do nothing.
	require.NoError(t, sys.abortMut(1, tx.mvid))
	v, err := cellValue(t, db, 1, tx.mvid)
	require.NoError(t, err)
	require.Equal(t, []byte("committed cell"), v)

	// Volume lock is held by nobody: new transaction begins immediately.
	txb2, err := vol.BeginTx(ctx, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	require.NoError(t, txb2.Abort(ctx))
}
