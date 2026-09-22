package bccore_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bccore"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/backend/memory"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
)

var errInjectedAbort = errors.New("injected abort failure")

// faultTx wraps a Tx and optionally fails Abort a configurable number of
// times. It can also block Get to exercise in-flight draining.
type faultTx struct {
	bccore.Tx
	vol *faultVolume
}

func (t *faultTx) Abort(ctx context.Context) error {
	if t.vol != nil {
		if n := t.vol.abortFailures.Load(); n > 0 {
			t.vol.abortFailures.Add(-1)
			return errInjectedAbort
		}
		if t.vol.onAbort != nil {
			t.vol.onAbort()
		}
	}
	return t.Tx.Abort(ctx)
}

func (t *faultTx) Get(ctx context.Context, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	if t.vol != nil && t.vol.onGet != nil {
		return t.vol.onGet(ctx, t.Tx, cid, buf, opts)
	}
	return t.Tx.Get(ctx, cid, buf, opts)
}

// faultVolume wraps a volume so that every transaction it begins is a
// faultTx. All other Volume methods delegate to the inner volume.
type faultVolume struct {
	bccore.Volume
	abortFailures atomic.Int32
	onAbort       func()
	onGet         func(ctx context.Context, inner bccore.Tx, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error)
}

func (v *faultVolume) BeginTx(ctx context.Context, spec blobcache.TxParams) (bccore.Tx, error) {
	inner, err := v.Volume.BeginTx(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &faultTx{Tx: inner, vol: v}, nil
}

func newFaultSystem(t testing.TB, abortFailures int32) (*bccore.System, *faultVolume) {
	t.Helper()
	vol := &faultVolume{Volume: memory.NewVolume(1 << 20)}
	vol.abortFailures.Store(abortFailures)
	s := bccore.New(bccore.Params{Root: vol})
	return &s, vol
}

func rootVolumeHandle(s *bccore.System) blobcache.Handle {
	return s.Mint(blobcache.OID{}, blobcache.Action_ALL, time.Now(), time.Hour)
}

// beginMutTx starts a mutating transaction on the root volume. The begin
// call uses a bounded context so that a leaked per-volume write lock turns
// into a test failure instead of an infinite hang.
func beginMutTx(t testing.TB, ctx context.Context, s *bccore.System) blobcache.Handle {
	t.Helper()
	beginCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	txh, err := s.BeginTx(beginCtx, rootVolumeHandle(s), blobcache.TxParams{Modify: true})
	require.NoError(t, err)
	require.NotNil(t, txh)
	return *txh
}

func loadRoot(t testing.TB, ctx context.Context, s *bccore.System, txh blobcache.Handle) []byte {
	t.Helper()
	var dst []byte
	require.NoError(t, s.Load(ctx, txh, &dst))
	if dst == nil {
		return []byte{}
	}
	return dst
}

// TestTxDroppedHandleReleasesVolume verifies that dropping the last handle
// to a mutating transaction runs the termination flow immediately, so that
// a new transaction on the same volume can start and does not see the
// dropped transaction's intermediate writes.
func TestTxDroppedHandleReleasesVolume(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	txh := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, txh, []byte("intermediate")))
	require.NoError(t, s.Drop(ctx, txh))

	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte{}, loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}

// TestTxExpiryCleanup verifies that expired transaction handles are
// terminated by Cleanup and the volume becomes writable again.
func TestTxExpiryCleanup(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	txh := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, txh, []byte("intermediate")))

	require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))

	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte{}, loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}

// TestTxCleanupReadOnly ensures read-only transactions are ended through
// their backend lifecycle as well (they hold a read lock / snapshot), and
// do not block a subsequent mutating transaction.
func TestTxCleanupReadOnly(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	roh, err := s.BeginTx(ctx, rootVolumeHandle(s), blobcache.TxParams{Modify: false})
	require.NoError(t, err)
	var dst []byte
	require.NoError(t, s.Load(ctx, *roh, &dst))
	require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))

	next := beginMutTx(t, ctx, s)
	require.NoError(t, s.Abort(ctx, next))
}

// TestCommittedTxNotRolledBackByCleanup ensures a successfully committed
// transaction is left committed even if Cleanup runs immediately after it.
func TestCommittedTxNotRolledBackByCleanup(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	txh := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, txh, []byte("committed")))
	require.NoError(t, s.Commit(ctx, txh))
	require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))

	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte("committed"), loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}

// TestKeepAlivePreventsExpiry verifies that KeepAlive extends an unexpired
// transaction, and fails once the handle has been reaped, so reclamation
// cannot be undone by a late KeepAlive.
func TestKeepAlivePreventsExpiry(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	txh := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, txh, []byte("kept alive")))

	require.NoError(t, s.KeepAlive(ctx, []blobcache.Handle{txh}))
	// The handle was just extended; an earlier cleanup must not reap it.
	require.NoError(t, s.Cleanup(ctx, time.Now().Add(30*time.Second), nil))
	require.Equal(t, []byte("kept alive"), loadRoot(t, ctx, s, txh))

	// A later cleanup expires and terminates the transaction.
	require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))
	require.True(t, blobcache.IsErrInvalidHandle(s.KeepAlive(ctx, []blobcache.Handle{txh})),
		"keepalive on a reaped handle must fail")

	// The volume is immediately writable and cannot see the old writes.
	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte{}, loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}

// TestAbortFailureRetainedAndRetried verifies that a failed backend Abort
// keeps the transaction around for a later Cleanup retry, while other
// orphan transactions can still be cleaned up independently.
func TestAbortFailureRetainedAndRetried(t *testing.T) {
	ctx := testutil.Context(t)
	s, root := newFaultSystem(t, 2)

	// A second, healthy volume is mounted so that cleanup can be shown to
	// progress independently of the failing root transaction.
	otherVol := memory.NewVolume(1 << 20)
	otherOID := blobcache.RandomOID()
	otherHandle, err := s.Create(ctx, otherOID, otherVol, blobcache.Action_ALL, time.Now(), time.Hour)
	require.NoError(t, err)

	rootTx := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, rootTx, []byte("root data")))
	otherTx, err := s.BeginTx(ctx, otherHandle, blobcache.TxParams{Modify: true})
	require.NoError(t, err)

	// Drop both: root Abort fails (1st failure), other Abort succeeds.
	require.NoError(t, s.Drop(ctx, rootTx))
	require.NoError(t, s.Drop(ctx, *otherTx))

	// The healthy volume must be writable right away.
	beginCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	otherNext, err := s.BeginTx(beginCtx, otherHandle, blobcache.TxParams{Modify: true})
	cancel()
	require.NoError(t, err)
	require.NoError(t, s.Abort(ctx, *otherNext))

	// Root still locked by the unresolved transaction.
	require.NoError(t, s.Cleanup(ctx, time.Now(), nil)) // 2nd failure, retained
	// After the backend recovers, the next cleanup converges.
	require.NoError(t, s.Cleanup(ctx, time.Now(), nil))

	// Now the root volume is writable and the old writes are gone.
	rootNext := beginMutTx(t, ctx, s)
	require.Equal(t, []byte{}, loadRoot(t, ctx, s, rootNext))
	require.NoError(t, s.Abort(ctx, rootNext))
	require.Zero(t, root.abortFailures.Load(), "the failing Abort should have been retried to success")
}

// TestTerminationDrainsInFlightOperations verifies the termination order:
// the backend Abort is only called after operations already inside the
// transaction have exited.
func TestTerminationDrainsInFlightOperations(t *testing.T) {
	ctx := testutil.Context(t)
	s, vol := newFaultSystem(t, 0)

	entered := make(chan struct{})
	releaseGet := make(chan struct{})
	var aborted atomic.Bool
	vol.onGet = func(ctx context.Context, inner bccore.Tx, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
		close(entered)
		select {
		case <-releaseGet:
		case <-ctx.Done():
		}
		return inner.Get(ctx, cid, buf, opts)
	}
	vol.onAbort = func() { aborted.Store(true) }

	txh := beginMutTx(t, ctx, s)
	getDone := make(chan struct{})
	go func() {
		_, _ = s.Get(ctx, txh, blobcache.CID{}, nil, blobcache.GetOpts{})
		close(getDone)
	}()
	<-entered

	dropDone := make(chan struct{})
	go func() {
		_ = s.Drop(ctx, txh)
		close(dropDone)
	}()

	// While the Get is in flight, Abort must not have run and Drop must wait.
	select {
	case <-dropDone:
		t.Fatal("Drop returned while an operation was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	require.False(t, aborted.Load(), "backend aborted before in-flight operation exited")

	close(releaseGet)
	<-getDone
	select {
	case <-dropDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Drop did not finish after the in-flight operation exited")
	}
	require.True(t, aborted.Load(), "backend Abort should run once operations drain")

	// Volume reusable immediately.
	next := beginMutTx(t, ctx, s)
	require.NoError(t, s.Abort(ctx, next))
}

// TestCommitRacesDrop verifies that Commit and Drop/termination converge
// to a single terminal state: the volume always ends either committed or
// aborted, never stuck locked and never rolled back after commit.
func TestCommitRacesDrop(t *testing.T) {
	ctx := testutil.Context(t)
	for i := 0; i < 30; i++ {
		s, _ := newFaultSystem(t, 0)
		txh := beginMutTx(t, ctx, s)
		data := []byte("racing commit")
		require.NoError(t, s.Save(ctx, txh, data))

		var wg sync.WaitGroup
		wg.Add(2)
		var commitErr error
		go func() {
			defer wg.Done()
			commitErr = s.Commit(ctx, txh)
		}()
		go func() {
			defer wg.Done()
			_ = s.Drop(ctx, txh)
		}()
		wg.Wait()
		require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))
		committed := commitErr == nil
		if !committed {
			// If Drop/cleanup won the race, the handle or transaction is
			// already gone by the time Commit runs.
			var doneErr blobcache.ErrTxDone
			require.True(t, errors.As(commitErr, &doneErr) || blobcache.IsErrInvalidHandle(commitErr),
				"unexpected commit result: %v", commitErr)
		}

		// The single-writer volume must be available regardless of outcome.
		next := beginMutTx(t, ctx, s)
		got := loadRoot(t, ctx, s, next)
		if committed {
			require.Equal(t, data, got, "committed data must survive")
		} else {
			require.Equal(t, []byte{}, got, "aborted transaction must leave no writes")
		}
		require.NoError(t, s.Abort(ctx, next))
	}
}

// TestCommittedReplicaCleanup verifies that, when one handle replica is
// dropped and another replica commits, cleanup never rolls the commit
// back.
func TestCommittedReplicaCleanup(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	h1 := beginMutTx(t, ctx, s)
	h2, err := s.Share(h1, blobcache.Action_ALL)
	if err != nil {
		t.Skipf("transaction handle cannot be shared: %v", err)
	}
	require.NoError(t, s.Save(ctx, h1, []byte("replica commit")))

	require.NoError(t, s.Drop(ctx, h1))
	require.NoError(t, s.Commit(ctx, h2))
	require.NoError(t, s.Cleanup(ctx, time.Now().Add(2*time.Minute), nil))

	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte("replica commit"), loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}

// TestAbortAllTerminatesTransactions verifies the Close path shares the
// convergent termination flow and releases single-writer volumes.
func TestAbortAllTerminatesTransactions(t *testing.T) {
	ctx := testutil.Context(t)
	s, _ := newFaultSystem(t, 0)

	txh := beginMutTx(t, ctx, s)
	require.NoError(t, s.Save(ctx, txh, []byte("shutdown")))
	require.NoError(t, s.AbortAll(ctx))

	next := beginMutTx(t, ctx, s)
	require.Equal(t, []byte{}, loadRoot(t, ctx, s, next))
	require.NoError(t, s.Abort(ctx, next))
}
