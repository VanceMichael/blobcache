package bcfuse

import (
	"context"
	"fmt"
	"io"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
)

// ReadFile reads up to len(dst) bytes of the file identified by id starting at
// off, overlaying extents that have been accepted by Write but not yet
// committed to the volume. Buffered bytes take precedence over committed
// bytes; sparse holes read back as zeros. It returns the number of bytes
// placed in dst (0 at or beyond the end of the file).
func (fs *FS[K]) ReadFile(ctx context.Context, id K, dst []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("bcfuse: negative read offset")
	}
	if len(dst) == 0 {
		return 0, nil
	}

	// Uncommitted view, across every batch (pending and committed-but-unconfirmed).
	buffered, bufEnd, err := fs.bufferedView(ctx, id)
	if err != nil {
		return 0, err
	}

	// Committed view.
	root := fs.currentRoot()
	tx, err := bcsdk.BeginTx(ctx, fs.svc, fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Abort(ctx) }()

	committed := make([]byte, len(dst))
	n, committedSize, err := fs.scheme.ReadFileAt(ctx, tx, root, id, committed, off)
	if err != nil {
		return 0, err
	}
	committed = committed[:n]

	viewEnd := committedSize
	if bufEnd > viewEnd {
		viewEnd = bufEnd
	}
	if off >= viewEnd {
		return 0, io.EOF
	}
	rn := int(viewEnd - off)
	if rn > len(dst) {
		rn = len(dst)
	}
	out := dst[:rn]
	// Committed bytes only cover [off, off+n); the remainder up to
	// committedSize is already zero in out.
	copy(out, committed)

	// Overlay buffered segments that intersect the read window.
	winStart := off
	winEnd := off + int64(rn)
	for _, s := range buffered {
		if s.end <= winStart || s.start >= winEnd {
			continue
		}
		start := s.start
		if start < winStart {
			start = winStart
		}
		end := s.end
		if end > winEnd {
			end = winEnd
		}
		srcOff := start - s.start
		dstOff := start - winStart
		copy(out[dstOff:dstOff+(end-start)], s.data[srcOff:srcOff+(end-start)])
	}
	return rn, nil
}

// bufferedView returns the coalesced buffered extents for a file and the end
// offset of the last buffered byte (exclusive).
func (fs *FS[K]) bufferedView(ctx context.Context, id K) ([]seg, int64, error) {
	rows, err := fs.db.QueryxContext(ctx,
		`SELECT start, "end", data FROM extents WHERE id = ? ORDER BY batch_id, start`, id)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var segs []seg
	var end int64
	for rows.Next() {
		var s, e int64
		var d []byte
		if err := rows.Scan(&s, &e, &d); err != nil {
			return nil, 0, err
		}
		segs = mergeSegs(segs, seg{start: s, end: e, data: d})
		if e > end {
			end = e
		}
	}
	return segs, end, rows.Err()
}

// FileSize returns the readable size of a file including buffered extents.
// Missing files have size 0.
func (fs *FS[K]) FileSize(ctx context.Context, id K) (int64, error) {
	_, bufEnd, err := fs.bufferedView(ctx, id)
	if err != nil {
		return 0, err
	}
	root := fs.currentRoot()
	tx, err := bcsdk.BeginTx(ctx, fs.svc, fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Abort(ctx) }()
	info, exists, err := fs.scheme.StatFile(ctx, tx, root, id)
	if err != nil {
		return 0, err
	}
	size := int64(0)
	if exists {
		size = info.Size
	}
	if bufEnd > size {
		return bufEnd, nil
	}
	return size, nil
}
