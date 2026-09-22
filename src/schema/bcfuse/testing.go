package bcfuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"blobcache.io/blobcache/src/internal/testutil"
	"blobcache.io/blobcache/src/schema"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// TestFS tests a filesystem implementation with the given scheme factory
func TestFS[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	t.Run("BasicOperations", func(t *testing.T) {
		testBasicOperations(t, newScheme)
	})

	t.Run("InodeMapping", func(t *testing.T) {
		testInodeMapping(t, newScheme)
	})

	t.Run("ExtentOperations", func(t *testing.T) {
		testExtentOperations(t, newScheme)
	})

	t.Run("FlushOperations", func(t *testing.T) {
		testFlushOperations(t, newScheme)
	})

	t.Run("ReadAfterWrite", func(t *testing.T) {
		testReadAfterWrite(t, newScheme)
	})

	t.Run("OverlappingAndSparseWrites", func(t *testing.T) {
		testOverlappingAndSparseWrites(t, newScheme)
	})

	t.Run("FlushRoundTrip", func(t *testing.T) {
		testFlushRoundTrip(t, newScheme)
	})

	t.Run("RecoverPendingBatches", func(t *testing.T) {
		testRecoverPendingBatches(t, newScheme)
	})

	t.Run("RecoverCommittedBatchesConfirmOnly", func(t *testing.T) {
		testRecoverCommittedBatches(t, newScheme)
	})

	t.Run("WriteFlushBatchBoundary", func(t *testing.T) {
		testWriteFlushBatchBoundary(t, newScheme)
	})

	t.Run("ConcurrentWritesAndFlushes", func(t *testing.T) {
		testConcurrentWritesAndFlushes(t, newScheme)
	})

	t.Run("ShutdownCancelledIsObservable", func(t *testing.T) {
		testShutdownCancelled(t, newScheme)
	})

	t.Run("ShutdownCleanAndClosed", func(t *testing.T) {
		testShutdownClean(t, newScheme)
	})

	t.Run("ReadOnly", func(t *testing.T) {
		testReadOnly(t, newScheme)
	})

	t.Run("DirectoryCreateAndRead", func(t *testing.T) {
		testDirectoryCreateAndRead(t, newScheme)
	})
}

// testBasicOperations tests basic filesystem operations
func testBasicOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	fsx := newTestFS(t, newScheme(t))

	// Test that we can create a filesystem
	require.NotNil(t, fsx)

	// Test that we can get the FUSE root
	root := fsx.FUSERoot()
	require.NotNil(t, root)
	require.Equal(t, int64(1), root.ino)
}

// testInodeMapping tests inode to ID mapping
func testInodeMapping[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	fsx := newTestFS(t, newScheme(t))

	// We need a test ID - this will depend on the concrete type K
	var testID K
	// Try to create a meaningful test ID based on the type
	switch any(testID).(type) {
	case string:
		testID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", testID)
		return
	}

	// Test that we can get an inode for an ID
	ino := fsx.GetOrCreateInode(testID)
	require.Greater(t, ino, int64(1)) // Should be > 1 (root)

	// Test that we get the same inode for the same ID
	ino2 := fsx.GetOrCreateInode(testID)
	require.Equal(t, ino, ino2)

	// Test reverse lookup
	id, exists := fsx.GetID(ino)
	require.True(t, exists)
	require.Equal(t, testID, id)

	// Test lookup of non-existent inode
	_, exists = fsx.GetID(9999)
	require.False(t, exists)
}

// testExtentOperations tests extent buffering operations
func testExtentOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))

	// We need a test ID
	var fileID K
	switch any(fileID).(type) {
	case string:
		fileID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", fileID)
		return
	}

	// Test putting an extent
	data := []byte("hello world")
	err := fsx.PutExtent(ctx, fileID, 0, data)
	require.NoError(t, err)

	// Test putting another extent
	data2 := []byte(" more data")
	err = fsx.PutExtent(ctx, fileID, int64(len(data)), data2)
	require.NoError(t, err)
}

// testFlushOperations tests flushing extents to the volume
func testFlushOperations[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))

	var fileID K
	switch any(fileID).(type) {
	case string:
		fileID = any("test-file.txt").(K)
	default:
		t.Skip("Cannot create test ID for type", fileID)
		return
	}

	// Put some extent data
	data := []byte("hello world")
	err := fsx.PutExtent(ctx, fileID, 0, data)
	require.NoError(t, err)

	// Test flushing extents
	err = fsx.Flush(ctx)
	require.NoError(t, err)
}

func testReadAfterWrite[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	var _ K
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	fileID := mustStringID[K](t, "read-after-write.txt")

	require.NoError(t, fsx.PutExtent(ctx, fileID, 0, []byte("hello")))

	// Immediate read at the same mount point must see the accepted write.
	got := readFS(t, ctx, fsx, fileID, 32)
	require.Equal(t, "hello", string(got))

	size, err := fsx.FileSize(ctx, fileID)
	require.NoError(t, err)
	require.Equal(t, int64(5), size)

	// Reading at an offset inside the buffered range.
	buf := make([]byte, 3)
	n, err := fsx.ReadFile(ctx, fileID, buf, 2)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "llo", string(buf))

	// Reading past the end reports EOF with no bytes.
	n, err = fsx.ReadFile(ctx, fileID, make([]byte, 4), 5)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
}

func testOverlappingAndSparseWrites[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	id := mustStringID[K](t, "merge.txt")

	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("abcdefghij")))
	// Overlapping write overrides the covered bytes.
	require.NoError(t, fsx.PutExtent(ctx, id, 2, []byte("XYZ")))
	require.Equal(t, "abXYZfghij", string(readFS(t, ctx, fsx, id, 16)))

	// Adjacent write coalesces.
	require.NoError(t, fsx.PutExtent(ctx, id, 10, []byte("K")))
	require.Equal(t, "abXYZfghijK", string(readFS(t, ctx, fsx, id, 16)))

	// Large overwrite extends and overrides.
	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("12345")))
	require.Equal(t, "12345fghijK", string(readFS(t, ctx, fsx, id, 16)))

	// Sparse write keeps a zero-filled hole.
	sparse := mustStringID[K](t, "sparse.txt")
	require.NoError(t, fsx.PutExtent(ctx, sparse, 0, []byte("hello")))
	require.NoError(t, fsx.PutExtent(ctx, sparse, 10, []byte("!")))
	got := readFS(t, ctx, fsx, sparse, 32)
	require.Equal(t, 11, len(got))
	require.Equal(t, "hello\x00\x00\x00\x00\x00!", string(got))
}

func testFlushRoundTrip[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	id := mustStringID[K](t, "roundtrip.txt")

	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("committed-data")))
	require.NoError(t, fsx.Flush(ctx))

	// The local buffer is empty after confirmation.
	n, err := fsx.pendingExtentCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	// Reads now come from the committed volume.
	require.Equal(t, "committed-data", string(readFS(t, ctx, fsx, id, 32)))

	size, err := fsx.FileSize(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(len("committed-data")), size)

	// An additional flush with nothing pending is a no-op.
	require.NoError(t, fsx.Flush(ctx))

	// A later buffered write overlays the committed content until flushed.
	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("XX")))
	require.Equal(t, "XXmmitted-data", string(readFS(t, ctx, fsx, id, 32)))
	require.NoError(t, fsx.Flush(ctx))
	require.Equal(t, "XXmmitted-data", string(readFS(t, ctx, fsx, id, 32)))
}

func testRecoverPendingBatches[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	db, svc, volh := newTestDeps(t)
	scheme0 := newScheme(t)
	fs1 := openFS(t, db, svc, volh, scheme0)
	id := mustStringID[K](t, "recover-pending.txt")

	require.NoError(t, fs1.PutExtent(ctx, id, 0, []byte("recover-me")))
	// Simulate process exit without unmount: no Flush; open a fresh FS on the
	// same buffer database and volume.
	fs2 := openFS(t, db, svc, volh, newScheme(t))

	// The accepted write is still visible through the recovered buffer.
	require.Equal(t, "recover-me", string(readFS(t, ctx, fs2, id, 32)))
	n, err := fs2.pendingExtentCount(ctx)
	require.NoError(t, err)
	require.NotZero(t, n)

	// Recovery policy: pending identity means the batch is committed now.
	require.NoError(t, fs2.Flush(ctx))
	require.Equal(t, "recover-me", string(readFS(t, ctx, fs2, id, 32)))
	n, err = fs2.pendingExtentCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}

func testRecoverCommittedBatches[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	db, svc, volh := newTestDeps(t)
	fs1 := openFS(t, db, svc, volh, newScheme(t))
	id := mustStringID[K](t, "acknowledged.txt")
	require.NoError(t, fs1.PutExtent(ctx, id, 0, []byte("on-volume")))
	require.NoError(t, fs1.Flush(ctx))
	committedRoot := fs1.currentRoot()
	require.NotEmpty(t, committedRoot)

	// Simulate "volume committed, local confirmation interrupted": a committed
	// batch row with a leftover extent.
	sqlutil.DoTx(ctx, db, func(tx *sqlx.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO batches (id, state, root, created_at, committed_at) VALUES (?, ?, ?, 0, 0)`,
			999, batchCommitted, committedRoot); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO extents (batch_id, id, start, "end", data) VALUES (?, ?, 0, 5, ?)`,
			999, "ghost-file", []byte("ghost"))
		return err
	})

	// A read-only mount must finish confirmation without applying anything:
	// no modify transaction is attempted.
	fsRO := openFS(t, db, svc, volh, newScheme(t), WithReadOnly[K]())
	n, err := fsRO.pendingExtentCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, "on-volume", string(readFS(t, ctx, fsRO, id, 32)))
}

func testWriteFlushBatchBoundary[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	inner := newScheme(t)
	gate := &flushGateScheme[K]{
		Scheme:  inner,
		inFlush: make(chan struct{}),
		release: make(chan struct{}),
	}
	db, svc, volh := newTestDeps(t)
	fsx := openFS(t, db, svc, volh, gate)
	id := mustStringID[K](t, "boundary.txt")

	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("AAAA")))

	flushErr := make(chan error, 1)
	go func() { flushErr <- fsx.Flush(ctx) }()
	select {
	case <-gate.inFlush:
	case <-time.After(10 * time.Second):
		t.Fatal("flush never reached the volume transaction")
	}

	// While the first batch is committing, a late write must be accepted into
	// the next batch without blocking, and must not be confirmed (deleted) by
	// the in-flight batch.
	require.NoError(t, fsx.PutExtent(ctx, id, 5, []byte("B")))

	close(gate.release)
	require.NoError(t, <-flushErr)

	require.Equal(t, "AAAA\x00B", string(readFS(t, ctx, fsx, id, 32)))

	// Final shutdown commits the late write as well.
	require.NoError(t, fsx.Shutdown(ctx))
	require.Equal(t, "AAAA\x00B", string(readFS(t, ctx, fsx, id, 32)))
	n, err := fsx.pendingExtentCount(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}

func testConcurrentWritesAndFlushes[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	id := mustStringID[K](t, "concurrent.txt")

	const goroutines = 8
	const region = 128
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Background flusher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				_ = fsx.Flush(ctx)
			}
		}
	}()

	var wgw sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wgw.Add(1)
		go func(g int) {
			defer wgw.Done()
			marker := byte('A' + g)
			payload := bytes.Repeat([]byte{marker}, region)
			base := g * region
			for round := 0; round < 10; round++ {
				if err := fsx.PutExtent(ctx, id, int64(base), payload); err != nil {
					panic(err)
				}
			}
		}(g)
	}
	wgw.Wait()
	close(stop)
	wg.Wait()

	require.NoError(t, fsx.Shutdown(ctx))

	got := readFS(t, ctx, fsx, id, goroutines*region)
	require.Equal(t, goroutines*region, len(got))
	for g := 0; g < goroutines; g++ {
		marker := byte('A' + g)
		for i := 0; i < region; i++ {
			require.Equal(t, marker, got[g*region+i], "offset %d", g*region+i)
		}
	}
}

func testShutdownCancelled[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	inner := newScheme(t)
	gate := &flushGateScheme[K]{
		Scheme:  inner,
		inFlush: make(chan struct{}),
		release: make(chan struct{}),
		cancelAware: true,
	}
	db, svc, volh := newTestDeps(t)
	fsx := openFS(t, db, svc, volh, gate)
	id := mustStringID[K](t, "shutdown-cancel.txt")
	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("never-lost")))

	sctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- fsx.Shutdown(sctx) }()
	select {
	case <-gate.inFlush:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown flush never started")
	}
	cancel()
	close(gate.release)
	select {
	case err := <-errCh:
		// The bounded attempt failed and the error is observable.
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled shutdown did not return in bounded time")
	}

	// Accepted writes survive: a fresh mount recovers the pending batch.
	fs2 := openFS(t, db, svc, volh, newScheme(t))
	require.Equal(t, "never-lost", string(readFS(t, ctx, fs2, id, 32)))
	require.NoError(t, fs2.Flush(ctx))
	require.Equal(t, "never-lost", string(readFS(t, ctx, fs2, id, 32)))
}

func testShutdownClean[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	id := mustStringID[K](t, "clean-shutdown.txt")
	require.NoError(t, fsx.PutExtent(ctx, id, 0, []byte("bye")))

	require.NoError(t, fsx.Shutdown(ctx))
	require.Equal(t, "bye", string(readFS(t, ctx, fsx, id, 32)))

	// Closed filesystem rejects writes and flushes.
	err := fsx.PutExtent(ctx, id, 0, []byte("x"))
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, fsx.Flush(ctx), ErrClosed)
	// Shutdown is idempotent.
	require.NoError(t, fsx.Shutdown(ctx))
}

func testReadOnly[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t), WithReadOnly[K]())
	id := mustStringID[K](t, "ro.txt")

	err := fsx.PutExtent(ctx, id, 0, []byte("nope"))
	require.ErrorIs(t, err, ErrReadOnly)

	// No pending data: flush is a compatible no-op.
	require.NoError(t, fsx.Flush(ctx))

	// Missing files read as empty.
	n, err := fsx.ReadFile(ctx, id, make([]byte, 8), 0)
	require.Zero(t, n)
	require.ErrorIs(t, err, io.EOF)
	size, err := fsx.FileSize(ctx, id)
	require.NoError(t, err)
	require.Zero(t, size)

	// A read-only mount refusing pending data must fail Init loudly.
	db, svc, volh := newTestDeps(t)
	rw := openFS(t, db, svc, volh, newScheme(t))
	require.NoError(t, rw.PutExtent(ctx, mustStringID[K](t, "pending.txt"), 0, []byte("x")))
	ro := New[K](db, svc, volh, newScheme(t), WithReadOnly[K]())
	err = ro.Init(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrReadOnly)
}

func testDirectoryCreateAndRead[K comparable](t *testing.T, newScheme func(testing.TB) Scheme[K]) {
	ctx := testutil.Context(t)
	fsx := newTestFS(t, newScheme(t))
	var zero K

	// Create a file through a modify transaction (same path as the FUSE Create).
	tx, err := bcsdk.BeginTx(ctx, fsx.svc, fsx.vol, blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	var root []byte
	require.NoError(t, tx.Load(ctx, &root))
	child, newRoot, err := fsx.scheme.CreateAt(ctx, tx, tx, root, zero, "created.txt", 0o644)
	require.NoError(t, err)
	require.NoError(t, tx.Save(ctx, newRoot))
	require.NoError(t, tx.Commit(ctx))
	fsx.mu.Lock()
	fsx.root = newRoot
	fsx.mu.Unlock()

	// Existing directory traversal still works.
	rtx, err := bcsdk.BeginTx(ctx, fsx.svc, fsx.vol, blobcache.TxParams{Modify: false})
	require.NoError(t, err)
	defer func() { _ = rtx.Abort(ctx) }()
	entries, err := fsx.scheme.ReadDir(ctx, rtx, fsx.currentRoot(), zero)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name)
	}
	require.Contains(t, names, "created.txt")

	// Writing to the created file and flushing works.
	require.NoError(t, fsx.PutExtent(ctx, child, 0, []byte("file-body")))
	require.NoError(t, fsx.Flush(ctx))
	require.Equal(t, "file-body", string(readFS(t, ctx, fsx, child, 32)))
}

// --- test infrastructure ---

func newTestFS[K comparable](t testing.TB, scheme0 Scheme[K], opts ...Option[K]) *FS[K] {
	db, svc, volh := newTestDeps(t)
	return openFS(t, db, svc, volh, scheme0, opts...)
}

func newTestDeps(t testing.TB) (*sqlx.DB, blobcache.Service, blobcache.Handle) {
	ctx := testutil.Context(t)
	// Use a file-backed database like production: in-memory SQLite databases are
	// scoped per connection, which does not survive connection-pool growth.
	db, err := sqlutil.OpenDB(filepath.Join(t.TempDir(), "bcfuse-test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc := bclocal.NewTestService(t)
	vol, err := svc.CreateVolume(ctx, nil, blobcache.DefaultLocalSpec())
	require.NoError(t, err)
	return db, svc, *vol
}

func openFS[K comparable](t testing.TB, db *sqlx.DB, svc blobcache.Service, volh blobcache.Handle, scheme0 Scheme[K], opts ...Option[K]) *FS[K] {
	ctx := testutil.Context(t)
	require.NoError(t, SetupDB(ctx, db))
	fsx := New(db, svc, volh, scheme0, opts...)
	require.NoError(t, fsx.Init(ctx))
	return fsx
}

func mustStringID[K comparable](t *testing.T, s string) K {
	t.Helper()
	var zero K
	switch any(zero).(type) {
	case string:
		return any(s).(K)
	default:
		t.Skipf("string identifiers not supported for %T", zero)
		return zero
	}
}

func readFS[K comparable](t *testing.T, ctx context.Context, fsx *FS[K], id K, max int) []byte {
	t.Helper()
	buf := make([]byte, max)
	n, err := fsx.ReadFile(ctx, id, buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}
	return buf[:n]
}

// flushGateScheme wraps a Scheme and blocks FlushExtents until released,
// allowing tests to pin a commit in flight.
type flushGateScheme[K comparable] struct {
	Scheme[K]
	inFlush     chan struct{}
	release     chan struct{}
	once        sync.Once
	cancelAware bool
}

func (g *flushGateScheme[K]) FlushExtents(ctx context.Context, dst schema.WO, src schema.RO, root []byte, extents []Extent[K]) ([]byte, error) {
	g.once.Do(func() { close(g.inFlush) })
	<-g.release
	if g.cancelAware && ctx.Err() != nil {
		return nil, fmt.Errorf("cancelled while committing: %w", ctx.Err())
	}
	return g.Scheme.FlushExtents(ctx, dst, src, root, extents)
}
