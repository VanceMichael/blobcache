package bcp

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/stretchr/testify/require"
)

func TestHeader(t *testing.T) {
	h := MessageHeader{}
	h.SetCode(MT_OPEN_FROM)
	h.SetBodyLen(10)

	require.Equal(t, h.Code(), MT_OPEN_FROM)
	require.Equal(t, h.BodyLen(), 10)
}

func TestMessageReadWrite(t *testing.T) {
	m := Message{}
	m.SetCode(MT_OPEN_FROM)
	m.SetBody([]byte("hello"))

	buf := &bytes.Buffer{}
	_, err := m.WriteTo(buf)
	require.NoError(t, err)

	m2 := Message{}
	_, err = m2.ReadFrom(buf)
	require.NoError(t, err)

	require.Equal(t, m2.Header().Code(), MT_OPEN_FROM)
	require.Equal(t, m2.Header().BodyLen(), len("hello"))
	require.Equal(t, string(m2.Body()), "hello")
}

func TestMessageRejectsOversizedBody(t *testing.T) {
	var hdr MessageHeader
	hdr.SetCode(MT_PING)
	hdr.SetBodyLen(MaxBodyLen + 1)

	var m Message
	_, err := m.ReadFrom(bytes.NewReader(hdr[:]))
	require.ErrorIs(t, err, ErrMessageTooLarge)
}

func TestPermissionWireError(t *testing.T) {
	m := Message{}
	err0 := blobcache.ErrPermission{
		Handle:   blobcache.Handle{OID: blobcache.OID{1}},
		Rights:   0x10,
		Requires: 0x20,
	}
	m.SetError(err0)
	require.Equal(t, MT_ERROR_NO_PERMISSION, m.Header().Code())

	err := parseWireError(m.Header().Code(), m.Body())
	var ep *blobcache.ErrPermission
	require.ErrorAs(t, err, &ep)
	require.Equal(t, err0.Rights, ep.Rights)
	require.Equal(t, err0.Requires, ep.Requires)
	require.Equal(t, err0.Handle, ep.Handle)
}

type streamGateHandler struct {
	started  chan struct{}
	finished chan struct{}
}

func (h *streamGateHandler) ServeBCP(ctx context.Context, from blobcache.Endpoint, req Message, resp *Message) bool {
	close(h.started)
	<-ctx.Done()
	close(h.finished)
	resp.SetCode(MT_OK)
	resp.SetBody(nil)
	return true
}

// A clean peer EOF converges without an error.
func TestServeStreamEOFQuiet(t *testing.T) {
	t.Parallel()
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- ServeStream(context.Background(), blobcache.Endpoint{}, b, &streamGateHandler{
			started: make(chan struct{}), finished: make(chan struct{}),
		})
	}()
	require.NoError(t, a.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeStream did not return after peer EOF")
	}
}

// An undeliverable frame is a protocol error and stays observable while
// the service context is alive.
func TestServeStreamOversizedFrameObservable(t *testing.T) {
	t.Parallel()
	a, b := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- ServeStream(context.Background(), blobcache.Endpoint{}, b, &streamGateHandler{
			started: make(chan struct{}), finished: make(chan struct{}),
		})
	}()
	var hdr MessageHeader
	hdr.SetCode(MT_PING)
	hdr.SetBodyLen(MaxBodyLen + 1)
	_, err := a.Write(hdr[:])
	require.NoError(t, err)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrMessageTooLarge)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeStream did not return after an oversized frame")
	}
	require.NoError(t, a.Close())
}

// A handler blocked while its caller disconnects must end with the
// request context canceled.
func TestServeStreamHandlerEndsOnPeerClose(t *testing.T) {
	t.Parallel()
	a, b := net.Pipe()
	h := &streamGateHandler{started: make(chan struct{}), finished: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- ServeStream(context.Background(), blobcache.Endpoint{}, b, h)
	}()

	var req Message
	req.SetCode(MT_PING)
	req.SetBody(nil)
	_, err := req.WriteTo(a)
	require.NoError(t, err)
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	require.NoError(t, a.Close())
	select {
	case <-h.finished:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked handler outlived the closed peer")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeStream did not return after peer close")
	}
}

// When the parent context is canceled, the connection is closed and
// ServeStream returns, even while the peer stays open.
func TestServeStreamStopsWithParentContext(t *testing.T) {
	t.Parallel()
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	h := &streamGateHandler{started: make(chan struct{}), finished: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- ServeStream(ctx, blobcache.Endpoint{}, b, h)
	}()

	var req Message
	req.SetCode(MT_PING)
	req.SetBody(nil)
	_, err := req.WriteTo(a)
	require.NoError(t, err)
	select {
	case <-h.started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()
	select {
	case <-h.finished:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked handler outlived the canceled parent context")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeStream did not return after parent context cancellation")
	}
	// The peer observes the closed connection (done implies the server
	// end was closed, so this read cannot block).
	one := make([]byte, 1)
	n, err := a.Read(one)
	require.Equal(t, 0, n)
	require.Error(t, err)
}
