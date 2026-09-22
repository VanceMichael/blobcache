package bcipc

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/bcp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/blobcache/blobcachetests"
	"blobcache.io/blobcache/src/internal/pools"
	"blobcache.io/blobcache/src/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	_ "blobcache.io/blobcache/src/schema/jsonns"
)

func waitForSocket(ctx context.Context, t testing.TB, sockPath string) {
	t.Helper()
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		// A successful dial proves the listener is actually accepting;
		// the socket file alone can exist briefly before listen is
		// ready or while a dead socket file lingers.
		if c, err := net.DialTimeout("unix", sockPath, time.Second); err == nil {
			_ = c.Close()
			return
		}
		if _, err := os.Stat(sockPath); err != nil && !os.IsNotExist(err) {
			require.NoError(t, err)
			return
		}
		select {
		case <-timeoutCtx.Done():
			require.NoError(t, timeoutCtx.Err())
			return
		case <-tick.C:
		}
	}
}

func TestService(t *testing.T) {
	t.Parallel()

	blobcachetests.ServiceAPI(t, func(t testing.TB) blobcache.Service {
		ctx := testutil.Context(t)
		ctx, cancel := context.WithCancel(ctx)

		// Keep the socket path short: deep subtest names plus the OS
		// temp dir can exceed the UNIX socket path length limit if the
		// socket is created in t.TempDir().
		sockPath := tempSockPath(t)
		svc := bclocal.NewTestService(t)

		var eg errgroup.Group
		eg.Go(func() error {
			return ListenAndServe(ctx, sockPath, &bcp.Server{
				Access: func(blobcache.NodeID) blobcache.Service {
					return svc
				},
			})
		})

		waitForSocket(ctx, t, sockPath)

		client := NewClient(sockPath)
		t.Cleanup(func() {
			cancel()
			_ = client.Close()
			_ = eg.Wait()
		})
		return client
	})
}

// tempSockPath returns a path short enough for UNIX sockets (on macOS
// SUN_LEN is limited to 104 bytes, while t.TempDir() plus a long test
// name can exceed that).
func tempSockPath(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bcipc-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}

// startServing listens on a fresh socket, serves h, and returns the
// socket path together with a wait function that reports Serve's return
// value (or times out).
func startServing(t testing.TB, ctx context.Context, h bcp.Handler) (string, func() error) {
	t.Helper()
	sockPath := tempSockPath(t)
	lis, err := Listen(sockPath)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, lis, h)
	}()
	waitForSocket(ctx, t, sockPath)
	wait := func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			return fmt.Errorf("bcipc: Serve did not return in time")
		}
	}
	return sockPath, wait
}

// pingHandler answers MT_PING with MT_OK; every other code gets a wire
// error, but the connection stays usable.
type pingHandler struct{}

func (pingHandler) ServeBCP(ctx context.Context, from blobcache.Endpoint, req bcp.Message, resp *bcp.Message) bool {
	if req.Header().Code() != bcp.MT_PING {
		resp.SetError(fmt.Errorf("unexpected message code: %v", req.Header().Code()))
		return true
	}
	resp.SetCode(bcp.MT_OK)
	resp.SetBody(nil)
	return true
}

// gateHandler blocks MT_PING until its handler context ends, and signals
// start/finish.  Other codes are answered immediately.
type gateHandler struct {
	started  chan struct{}
	finished chan struct{}
}

func (h *gateHandler) ServeBCP(ctx context.Context, from blobcache.Endpoint, req bcp.Message, resp *bcp.Message) bool {
	resp.SetCode(bcp.MT_OK)
	resp.SetBody(nil)
	if req.Header().Code() == bcp.MT_PING {
		close(h.started)
		<-ctx.Done()
		close(h.finished)
	}
	return true
}

func waitSignal(t testing.TB, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

// ask sends one raw request and returns the response code.
func ask(ctx context.Context, asker bcp.Asker, code bcp.MessageCode) (bcp.MessageCode, error) {
	var req, resp bcp.Message
	req.SetCode(code)
	if err := asker.Ask(ctx, blobcache.Endpoint{}, req, &resp); err != nil {
		return 0, err
	}
	return resp.Header().Code(), nil
}

// Canceling the service context must stop Serve even while a client
// keeps an idle connection pooled.
func TestServeStopsWithIdleConnInPool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(testutil.Context(t))

	sockPath, waitServe := startServing(t, ctx, pingHandler{})
	client := NewClient(sockPath)

	code, err := ask(ctx, &client.tp, bcp.MT_PING)
	require.NoError(t, err)
	require.Equal(t, bcp.MT_OK, code)
	require.Equal(t, 1, client.pool.Len())

	cancel()
	require.NoError(t, waitServe())
	require.NoError(t, client.Close())
}

// A call whose context is canceled after it took a connection must end
// promptly, the blocked server handler must also end, and a following
// normal request must succeed over a fresh connection.
func TestAskCanceledDuringBlockingRequest(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	h := &gateHandler{started: make(chan struct{}), finished: make(chan struct{})}
	sockPath, _ := startServing(t, ctx, h)
	client := NewClient(sockPath)
	t.Cleanup(func() { _ = client.Close() })

	callCtx, callCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := ask(callCtx, &client.tp, bcp.MT_PING)
		errCh <- err
	}()

	waitSignal(t, h.started, "server handler never started")
	callCancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Ask did not unblock after context cancellation")
	}
	waitSignal(t, h.finished, "blocked server handler outlived the canceled caller")

	// Recovery: new request on a fresh connection succeeds.
	code, err := ask(ctx, &client.tp, bcp.MT_ENDPOINT)
	require.NoError(t, err)
	require.Equal(t, bcp.MT_OK, code)
}

// Close must tear down idle and in-flight connections, be idempotent,
// unblock blocked calls, and refuse further Take/Give resurrection.
func TestClientCloseLifecycle(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	h := &gateHandler{started: make(chan struct{}), finished: make(chan struct{})}
	sockPath, _ := startServing(t, ctx, h)
	client := NewClient(sockPath)

	// Warm an idle pooled connection.
	_, err := ask(ctx, &client.tp, bcp.MT_ENDPOINT)
	require.NoError(t, err)
	require.Equal(t, 1, client.pool.Len())

	errCh := make(chan error, 1)
	go func() {
		_, err := ask(ctx, &client.tp, bcp.MT_PING)
		errCh <- err
	}()
	waitSignal(t, h.started, "server handler never started")

	closeErr := make(chan error, 1)
	go func() { closeErr <- client.Close() }()
	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Client.Close blocked while a call was in flight")
	}
	select {
	case err := <-errCh:
		// The force-closed connection fails the blocked call rather than
		// leaving it stuck.
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight call was not unblocked by Client.Close")
	}
	waitSignal(t, h.finished, "server handler outlived Client.Close")

	// Repeated Close is a quiet no-op.
	require.NoError(t, client.Close())

	// Further calls fail deterministically and never reopen the pool.
	_, err = ask(ctx, &client.tp, bcp.MT_ENDPOINT)
	require.ErrorIs(t, err, pools.ErrClosed)
}

// A connection returned to a pool while it is closing must be closed,
// never left in the freelist.
func TestGiveRacesClose(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	sockPath, _ := startServing(t, ctx, pingHandler{})

	for range 50 {
		client := NewClient(sockPath)
		_, err := ask(ctx, &client.tp, bcp.MT_ENDPOINT)
		require.NoError(t, err)
		conn, err := client.pool.Take(ctx)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = client.pool.Give(conn, true)
		}()
		go func() {
			defer wg.Done()
			_ = client.Close()
		}()
		wg.Wait()

		require.Zero(t, client.pool.Len())
		_, err = client.pool.Take(ctx)
		require.ErrorIs(t, err, pools.ErrClosed)
		// The leased connection is closed regardless of which goroutine won.
		_, err = conn.Write([]byte("x"))
		require.Error(t, err)
	}
}

// A frame claiming an oversized body is protocol damage: the server must
// close the offending connection, while a different client's calls keep
// working.
func TestOversizedFrameClosesConn(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	sockPath, _ := startServing(t, ctx, pingHandler{})
	client := NewClient(sockPath)
	t.Cleanup(func() { _ = client.Close() })

	raw, err := net.Dial("unix", sockPath)
	require.NoError(t, err)
	var hdr bcp.MessageHeader
	hdr.SetCode(bcp.MT_PING)
	hdr.SetBodyLen(bcp.MaxBodyLen + 1)
	_, err = raw.Write(hdr[:])
	require.NoError(t, err)
	require.NoError(t, raw.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, 1)
	n, err := raw.Read(buf)
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, raw.Close())

	// Recovery: the legitimate client (own connection) still works.
	code, err := ask(ctx, &client.tp, bcp.MT_PING)
	require.NoError(t, err)
	require.Equal(t, bcp.MT_OK, code)
}

// An in-band error reply keeps framing synchronized, so the connection
// stays healthy in the pool and serves following requests.
func TestSemanticErrorKeepsConnectionHealthy(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	sockPath, _ := startServing(t, ctx, pingHandler{})
	client := NewClient(sockPath)
	t.Cleanup(func() { _ = client.Close() })

	for range 2 {
		code, err := ask(ctx, &client.tp, bcp.MessageCode(0x00FE))
		require.NoError(t, err)
		require.True(t, code.IsError())
		require.Equal(t, 1, client.pool.Len())
	}
	code, err := ask(ctx, &client.tp, bcp.MT_PING)
	require.NoError(t, err)
	require.Equal(t, bcp.MT_OK, code)
	require.Equal(t, 1, client.pool.Len())
}

// Repeatedly starting and stopping the service on the same socket path
// must succeed deterministically.
func TestServeRestart(t *testing.T) {
	t.Parallel()
	base := testutil.Context(t)
	sockPath := tempSockPath(t)

	for range 3 {
		ctx, cancel := context.WithCancel(base)
		lis, err := Listen(sockPath)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- Serve(ctx, lis, pingHandler{}) }()
		waitForSocket(base, t, sockPath)

		client := NewClient(sockPath)
		code, err := ask(base, &client.tp, bcp.MT_PING)
		require.NoError(t, err)
		require.Equal(t, bcp.MT_OK, code)
		require.NoError(t, client.Close())

		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not stop")
		}
		_, err = os.Stat(sockPath)
		require.True(t, os.IsNotExist(err), "socket file should be unlinked when the listener closes")
	}
}

// Closing the listener externally (without context cancellation) is a
// real error and must remain observable.
func TestServeReturnsAcceptError(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	sockPath := tempSockPath(t)
	lis, err := Listen(sockPath)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, lis, pingHandler{}) }()
	waitForSocket(ctx, t, sockPath)

	require.NoError(t, lis.Close())
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after the listener was closed")
	}
}

// dequeueWatcher reports when a dequeue handler started and when it
// returned due to context cancellation.
type dequeueWatcher struct {
	bcp.Handler
	started  chan struct{}
	finished chan struct{}

	startOnce  sync.Once
	finishOnce sync.Once
}

func (w *dequeueWatcher) ServeBCP(ctx context.Context, ep blobcache.Endpoint, req bcp.Message, resp *bcp.Message) bool {
	isDequeue := req.Header().Code() == bcp.MT_QUEUE_DEQUEUE
	if isDequeue {
		w.startOnce.Do(func() { close(w.started) })
	}
	ok := w.Handler.ServeBCP(ctx, ep, req, resp)
	if isDequeue && ctx.Err() != nil {
		w.finishOnce.Do(func() { close(w.finished) })
	}
	return ok
}

// Canceling a blocked Dequeue must end the server-side waiter before a
// later message is enqueued, so the canceled request cannot consume it;
// the following dequeue then deterministically receives the message.
func TestDequeueCanceledDoesNotConsume(t *testing.T) {
	t.Parallel()
	ctx := testutil.Context(t)
	svc := bclocal.NewTestService(t)
	watcher := &dequeueWatcher{
		Handler:  NewServer(svc),
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	sockPath, _ := startServing(t, ctx, watcher)
	client := NewClient(sockPath)
	t.Cleanup(func() { _ = client.Close() })

	qh, err := client.CreateQueue(ctx, nil, blobcache.QueueSpec{
		Memory: &blobcache.QueueBackend_Memory{
			MaxDepth:             16,
			MaxBytesPerMessage:   1024,
			MaxHandlesPerMessage: 16,
		},
	})
	require.NoError(t, err)

	dctx, dcancel := context.WithCancel(ctx)
	defer dcancel()
	errCh := make(chan error, 1)
	go func() {
		buf := make([]blobcache.Message, 1)
		_, err := client.Dequeue(dctx, *qh, buf, blobcache.DequeueOpts{Min: 1})
		errCh <- err
	}()
	waitSignal(t, watcher.started, "dequeue handler never started")

	dcancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Dequeue did not return after cancellation")
	}
	waitSignal(t, watcher.finished, "server-side dequeue waiter outlived cancellation")

	_, err = client.Enqueue(ctx, *qh, []blobcache.Message{{Bytes: []byte("survives")}})
	require.NoError(t, err)

	buf := make([]blobcache.Message, 1)
	n, err := client.Dequeue(ctx, *qh, buf, blobcache.DequeueOpts{Min: 1})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte("survives"), buf[0].Bytes)

	// Nothing lingers in the queue behind the canceled waiter.
	n, err = client.Dequeue(ctx, *qh, buf, blobcache.DequeueOpts{})
	require.NoError(t, err)
	require.Equal(t, 0, n)
}
