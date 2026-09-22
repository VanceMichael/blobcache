package bclocal

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bclocal/internal/dbtab"
	"blobcache.io/blobcache/src/bclocal/internal/localvol"
	"blobcache.io/blobcache/src/bclocal/internal/pdb"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func noneVolumeHandle(t testing.TB, svc *Service) (blobcache.Handle, localvol.ID) {
	t.Helper()
	ctx := testutil.Context(t)
	spec := blobcache.VolumeSpec{
		Local: &blobcache.VolumeBackend_Local{
			HashAlgo: blobcache.HashAlgo_BLAKE3_256,
			MaxSize:  1 << 20,
		},
	}
	h, err := svc.CreateVolume(ctx, nil, spec)
	require.NoError(t, err)
	salt, err := getOIDSalt(svc.db)
	require.NoError(t, err)
	lvid, err := localvol.LocalIDFromOID(salt, h.OID)
	require.NoError(t, err)
	return *h, lvid
}

func beginMutTxBounded(t testing.TB, svc *Service, volh blobcache.Handle) blobcache.Handle {
	t.Helper()
	ctx, cancel := context.WithTimeout(testutil.Context(t), 5*time.Second)
	defer cancel()
	txh, err := svc.BeginTx(ctx, volh, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	return *txh
}

func localVolTxnKey(volID localvol.ID) []byte {
	return pdb.TKey{
		TableID: dbtab.TID_LOCAL_VOLUME_TXNS,
		Key:     volID.Marshal(nil),
	}.Marshal(nil)
}

func activeTxCount(t testing.TB, svc *Service) int {
	t.Helper()
	sn := svc.db.NewSnapshot()
	defer sn.Close()
	active := make(map[pdb.MVTag]struct{})
	require.NoError(t, svc.txSys.ReadActive(sn, active))
	return len(active)
}

func volumeTxnRecordExists(t testing.TB, svc *Service, volID localvol.ID) bool {
	t.Helper()
	sn := svc.db.NewSnapshot()
	defer sn.Close()
	_, closer, err := sn.Get(localVolTxnKey(volID))
	if err != nil {
		require.ErrorIs(t, err, pebble.ErrNotFound)
		return false
	}
	closer.Close()
	return true
}

func blobRefCount(t testing.TB, svc *Service, cid blobcache.CID) uint32 {
	t.Helper()
	k := pdb.TKey{TableID: dbtab.TID_BLOB_REF_COUNT, Key: cid[:16]}.Marshal(nil)
	v, closer, err := svc.db.Get(k)
	if err != nil {
		require.ErrorIs(t, err, pebble.ErrNotFound)
		return 0
	}
	defer closer.Close()
	require.Len(t, v, 4)
	return binary.BigEndian.Uint32(v)
}

// TestLocalTxCleanupOnDrop verifies the full local backend cleanup of a
// mutating transaction whose last handle is dropped: the per-volume write
// lock is released, the active transaction records are revoked, MVCC
// writes become invisible and blob reference counts are undone.
func TestLocalTxCleanupOnDrop(t *testing.T) {
	ctx := testutil.Context(t)
	svc := NewTestService(t)
	volH, lvid := noneVolumeHandle(t, svc)

	txh := beginMutTxBounded(t, svc, volH)
	require.NoError(t, svc.Save(ctx, txh, []byte("intermediate")))
	data := []byte("dropped transaction blob")
	cid, err := svc.Post(ctx, txh, data, blobcache.PostOpts{})
	require.NoError(t, err)
	require.NoError(t, svc.Drop(ctx, txh))

	// A new mutating transaction on the same volume starts immediately:
	// the per-volume write lock was released by the termination flow.
	next := beginMutTxBounded(t, svc, volH)
	var root []byte
	require.NoError(t, svc.Load(ctx, next, &root))
	require.Empty(t, root, "aborted cell write must be invisible")
	var exists blobcache.BitMap
	require.NoError(t, svc.Exists(ctx, next, []blobcache.CID{cid}, &exists))
	require.False(t, exists.IsSet(0), "aborted blob write must be invisible")
	require.NoError(t, svc.Abort(ctx, next))

	require.Zero(t, activeTxCount(t, svc), "SYS_TXNS active record must be revoked")
	require.False(t, volumeTxnRecordExists(t, svc, lvid), "LOCAL_VOLUME_TXNS record must be revoked")
	require.Zero(t, blobRefCount(t, svc, cid), "aborted blob reference must be released")
}

// TestLocalTxCleanupPostThenDelete verifies reference counts balance when
// a transaction posts and then deletes the same blob before being dropped.
func TestLocalTxCleanupPostThenDelete(t *testing.T) {
	ctx := testutil.Context(t)
	svc := NewTestService(t)
	volH, _ := noneVolumeHandle(t, svc)

	txh := beginMutTxBounded(t, svc, volH)
	data := []byte("post then delete")
	cid, err := svc.Post(ctx, txh, data, blobcache.PostOpts{})
	require.NoError(t, err)
	require.NoError(t, svc.Delete(ctx, txh, []blobcache.CID{cid}))
	require.NoError(t, svc.Drop(ctx, txh))

	// Volume writable again and the reference count leaked neither way.
	next := beginMutTxBounded(t, svc, volH)
	require.NoError(t, svc.Abort(ctx, next))
	require.Zero(t, blobRefCount(t, svc, cid))
}

// TestLocalTxCleanupLeavesUnexpiredAlone verifies that periodic Cleanup
// does not touch a transaction whose handle is still alive, and that the
// transaction is then cleanly terminated by Drop.
func TestLocalTxCleanupLeavesUnexpiredAlone(t *testing.T) {
	ctx := testutil.Context(t)
	svc := NewTestService(t)
	volH, _ := noneVolumeHandle(t, svc)

	txh := beginMutTxBounded(t, svc, volH)
	require.NoError(t, svc.Save(ctx, txh, []byte("unexpired")))
	require.NoError(t, svc.Cleanup(ctx))
	var root []byte
	require.NoError(t, svc.Load(ctx, txh, &root))
	require.Equal(t, []byte("unexpired"), root)

	// Dropping the last handle runs the same convergent termination flow.
	require.NoError(t, svc.Drop(ctx, txh))
	_, err := svc.InspectTx(ctx, txh)
	require.Error(t, err)

	next := beginMutTxBounded(t, svc, volH)
	var nextRoot []byte
	require.NoError(t, svc.Load(ctx, next, &nextRoot))
	require.Empty(t, nextRoot)
	require.NoError(t, svc.Abort(ctx, next))
	require.Zero(t, activeTxCount(t, svc))
}

// TestLocalTxCommitRevokesRecords verifies that committing a mutating
// transaction revokes the per-volume active record and remains durable
// against later cleanup, with the write lock released.
func TestLocalTxCommitRevokesRecords(t *testing.T) {
	ctx := testutil.Context(t)
	svc := NewTestService(t)
	volH, lvid := noneVolumeHandle(t, svc)

	txh := beginMutTxBounded(t, svc, volH)
	require.NoError(t, svc.Save(ctx, txh, []byte("final")))
	require.NoError(t, svc.Commit(ctx, txh))

	require.False(t, volumeTxnRecordExists(t, svc, lvid))
	require.Zero(t, activeTxCount(t, svc))

	require.NoError(t, svc.Cleanup(ctx))
	next := beginMutTxBounded(t, svc, volH)
	var root []byte
	require.NoError(t, svc.Load(ctx, next, &root))
	require.Equal(t, []byte("final"), root)
	require.NoError(t, svc.Abort(ctx, next))
}
