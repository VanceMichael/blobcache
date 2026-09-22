package bcipc

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"blobcache.io/blobcache/src/bcp"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

func Listen(p string) (*net.UnixListener, error) {
	laddr := net.UnixAddr{Name: p, Net: "unix"}
	return net.ListenUnix("unix", &laddr)
}

func ListenAndServe(ctx context.Context, p string, srv bcp.Handler) error {
	lis, err := Listen(p)
	if err != nil {
		return err
	}
	defer lis.Close()
	return Serve(ctx, lis, srv)
}

// Serve accepts connections on lis and serves BCP until ctx is canceled
// or Accept fails.
//
// When ctx is canceled the listener and every accepted connection are
// closed, including connections clients have left idle in a pool:
// in-flight handler contexts are canceled and blocked I/O is unblocked.
// Serve therefore returns in bounded time, with nil, after cancellation.
// A client EOF or an induced closed-connection error also converges
// quietly.  Genuine listener, protocol and network errors are returned
// (per-connection errors are logged) and remain observable.
func Serve(ctx context.Context, lis *net.UnixListener, srv bcp.Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	conns := &connTracker{}
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = lis.Close()
		conns.closeAll()
	}()

	for {
		uc, err := lis.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		conns.add(uc)
		wg.Go(func() {
			defer conns.remove(uc)
			defer uc.Close()
			if err := bcp.ServeStream(ctx, blobcache.Endpoint{}, uc, srv); err != nil {
				if ctx.Err() == nil &&
					!errors.Is(err, io.EOF) &&
					!errors.Is(err, net.ErrClosed) {
					logctx.Error(ctx, "while serving:", zap.Error(err))
				}
			}
		})
	}
}

// connTracker tracks accepted connections so they can all be closed on
// shutdown.
type connTracker struct {
	mu    sync.Mutex
	conns map[*net.UnixConn]struct{}
}

func (t *connTracker) add(c *net.UnixConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conns == nil {
		t.conns = make(map[*net.UnixConn]struct{})
	}
	t.conns[c] = struct{}{}
}

func (t *connTracker) remove(c *net.UnixConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, c)
}

func (t *connTracker) closeAll() {
	t.mu.Lock()
	conns := t.conns
	t.conns = make(map[*net.UnixConn]struct{})
	t.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

type Server = bcp.Server

func NewServer(svc blobcache.Service) *Server {
	return &bcp.Server{
		Access: func(ep blobcache.NodeID) blobcache.Service {
			return svc
		},
	}
}
