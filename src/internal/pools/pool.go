package pools

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by Take when the pool has been closed.
var ErrClosed = errors.New("pools: pool is closed")

// OpenClose is a pool of resources which have
// an open and close cost to be amoritized by staying in the pool.
//
// A resource is either idle (in the freelist) or leased (taken by a
// caller and not yet returned).  Close closes resources in both states,
// and it is safe to run concurrently with Take and Give.
type OpenClose[T comparable] struct {
	mu       sync.Mutex
	freelist chan T
	leased   map[T]struct{}
	closed   bool

	mk      func(context.Context) (T, error)
	closeFn func(T) error
}

func NewOpenClose[T comparable](n int, mk func(ctx context.Context) (T, error), closeFn func(T) error) OpenClose[T] {
	return OpenClose[T]{
		freelist: make(chan T, n),
		leased:   make(map[T]struct{}),
		mk:       mk,
		closeFn:  closeFn,
	}
}

// Take returns an idle resource, or opens a new one.
// It returns ErrClosed if the pool is closed.
func (p *OpenClose[T]) Take(ctx context.Context) (T, error) {
	var zero T

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return zero, ErrClosed
	}
	select {
	case x := <-p.freelist:
		p.leased[x] = struct{}{}
		p.mu.Unlock()
		return x, nil
	default:
	}
	p.mu.Unlock()

	x, err := p.mk(ctx)
	if err != nil {
		return zero, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = p.closeFn(x)
		return zero, ErrClosed
	}
	p.leased[x] = struct{}{}
	p.mu.Unlock()
	return x, nil
}

// Give returns a leased resource to the pool.
//
// If healthy is false the resource is closed instead of being reused,
// so canceled, timed-out, corrupted and failed I/O connections cannot
// pollute later calls.  Resources returned after Close are also closed.
// Give never blocks.
func (p *OpenClose[T]) Give(x T, healthy bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.leased, x)
	if !healthy || p.closed {
		return p.closeFn(x)
	}
	// The decision and the channel send must be atomic against Close,
	// otherwise Close can drain the freelist in between and the resource
	// gets resurrected after the pool is closed.
	select {
	case p.freelist <- x:
		return nil
	default:
		return p.closeFn(x)
	}
}

// Close marks the pool closed and closes every resource it currently
// holds, both idle and leased.  Leased resources are closed even while
// their callers may still be blocked in I/O, which unblocks those
// callers.  Resources opened or returned concurrently with Close are
// closed by Take/Give instead of leaking into the freelist.
//
// Close is idempotent.
func (p *OpenClose[T]) Close() (retErr error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	idle := make([]T, 0, len(p.freelist))
drain:
	for {
		select {
		case x := <-p.freelist:
			idle = append(idle, x)
		default:
			break drain
		}
	}
	leased := make([]T, 0, len(p.leased))
	for x := range p.leased {
		leased = append(leased, x)
	}
	clear(p.leased)
	p.mu.Unlock()

	for _, x := range idle {
		retErr = errors.Join(retErr, p.closeFn(x))
	}
	for _, x := range leased {
		retErr = errors.Join(retErr, p.closeFn(x))
	}
	return retErr
}

func (p *OpenClose[T]) Len() int {
	return len(p.freelist)
}

func (p *OpenClose[T]) Cap() int {
	return cap(p.freelist)
}
