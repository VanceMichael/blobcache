package bcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"blobcache.io/blobcache/src/blobcache"
)

type Handler interface {
	ServeBCP(ctx context.Context, from blobcache.Endpoint, req Message, resp *Message) bool
}

// ServeStream serves BCP over a bidi-stream
//
// The context passed to the handler is a child of ctx which is canceled
// when ctx is canceled, the peer disconnects, or an undeliverable frame
// is received.  As a result a request blocked inside the handler (for
// example a long-poll Dequeue) ends as soon as its caller goes away or
// the service stops, instead of lingering and consuming backend state.
//
// BCP requests on a stream are strictly sequential.  While a handler is
// running a dedicated reader is blocked waiting for the next frame, so
// a peer close (EOF/reset) is observed even while the handler is blocked.
func ServeStream(ctx context.Context, ep blobcache.Endpoint, conn io.ReadWriteCloser, srv Handler) error {
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	requests := make(chan Message)
	var readErr error
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		defer close(requests)
		for {
			var req Message
			if _, err := req.ReadFrom(conn); err != nil {
				// Peer gone, frame truncated/oversized, or conn closed by
				// shutdown: cancel any in-flight handler and stop reading.
				readErr = err
				cancel()
				return
			}
			select {
			case requests <- req:
			case <-ctx.Done():
				return
			}
		}
	}()
	// On exit make sure the reader is unblocked and reaped, so neither
	// the goroutine nor the connection outlives the stream.
	defer func() {
		cancel()
		_ = conn.Close()
		readerWG.Wait()
	}()

	var resp Message
	for {
		select {
		case <-ctx.Done():
			// Service shutdown closes the connection via the deferred
			// Close, unblocking the reader.  If only the child context
			// ended (the reader hit an error with the parent still
			// alive), surface that error instead of masking it: a clean
			// EOF stays quiet, a frame/network error is returned.
			if parentCtx.Err() != nil {
				return nil
			}
			readerWG.Wait()
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		case req, ok := <-requests:
			if !ok {
				// A clean peer EOF, or a read failure induced by the
				// caller shutting down, converges quietly.  Frame and
				// network errors while the parent context is alive are
				// returned to the caller so they stay observable.
				if errors.Is(readErr, io.EOF) || parentCtx.Err() != nil {
					return nil
				}
				return readErr
			}
			if srv.ServeBCP(ctx, ep, req, &resp) {
				// The caller is already gone or the service is stopping:
				// do not block writing a response nobody will receive.
				if ctx.Err() != nil {
					return nil
				}
				if _, err := resp.WriteTo(conn); err != nil {
					// The peer went away while the response was being
					// written: converge quietly rather than surfacing
					// the induced write failure.
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
			} else {
				// Handler refuses to serve this peer: hang up immediately.
				return nil
			}
		}
	}
}

func Serve(ctx context.Context, lis net.Listener, srv Handler) error {
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	wg := sync.WaitGroup{}
	for {
		conn, err := lis.Accept()
		if err != nil {
			cf()
			wg.Wait()
			return err
		}
		wg.Go(func() {
			defer conn.Close()
			defer cf()
			ServeStream(ctx, blobcache.Endpoint{}, conn, srv)
		})
		wg.Go(func() {
			<-ctx.Done()
			conn.Close()
		})
	}
}
