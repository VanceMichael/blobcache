// Package bcipc implements the Blobcache Protocol over UNIX sockets
package bcipc

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"blobcache.io/blobcache/src/bcp"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/pools"
)

// MaxMessageLen is the largest frame accepted on an IPC connection.
const MaxMessageLen = bcp.MaxBodyLen + bcp.HeaderLen

var _ bcp.Asker = &clientTransport{}

type clientTransport struct {
	pool   *pools.OpenClose[*net.UnixConn]
	bufLen int
}

func (ct *clientTransport) Ask(ctx context.Context, remEp blobcache.Endpoint, req bcp.Message, resp *bcp.Message) error {
	if remEp != (blobcache.Endpoint{}) {
		return fmt.Errorf("bcipc: endpoint must be zeroed in call to Ask.  HAVE: %v", remEp)
	}
	conn, err := ct.pool.Take(ctx)
	if err != nil {
		return err
	}

	// Once the connection is taken blocking I/O no longer notices
	// context cancellation on its own.  Close the connection from the
	// watcher when ctx ends so the write/read below unblocks promptly.
	var connCanceled atomic.Bool
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			connCanceled.Store(true)
			_ = conn.Close()
		case <-stop:
		}
	}()

	// The connection is reusable only when the full request/response
	// cycle completed without interruption.  Stop the watcher and wait
	// for it before returning the connection, so it can never be closed
	// out from under the next call that reuses it.
	healthy := false
	defer func() {
		close(stop)
		<-stopped
		_ = ct.pool.Give(conn, healthy && !connCanceled.Load())
	}()

	if _, err := req.WriteTo(conn); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	}
	if _, err := resp.ReadFrom(conn); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		return err
	}
	healthy = true
	return nil
}
