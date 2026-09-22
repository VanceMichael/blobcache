// Package bcfuse implements a filesystem interface for blobcache.
// bcfuse handles the frontend components of a filesystem, like FUSE and NFS mounting.
// The backend is provided by a blobcache volume.
// Translating the filesystem operations into blobcache operations is handled by a Scheme.
// bcfuse provides write buffering before the data is flushed to the volume.
package bcfuse

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"blobcache.io/blobcache/src/schema"
	"github.com/jmoiron/sqlx"
)

// Scheme handles filesystem operations on blobcache volumes.
// All reading methods follow the pattern: func(ctx context.Context, src schema.RO, root []byte, ...) (..., error)
// All writing methods follow the pattern: func(ctx context.Context, dst schema.WO, src schema.RO, root []byte, ...) (..., []byte, error)
type Scheme[K comparable] interface {
	// FlushExtents writes the extents in one committed batch to the volume.
	// Extents for the same file identifier are sorted by offset and do not
	// overlap: they represent a complete overlay on top of the file as it
	// exists at root.
	FlushExtents(ctx context.Context, dst schema.WO, src schema.RO, root []byte, extents []Extent[K]) ([]byte, error)

	// ReadFileAt reads up to len(dst) bytes of a file starting at off.
	// It returns the number of bytes placed in dst and the total committed
	// size of the file. A file which does not exist reads as empty: n == 0,
	// size == 0 and no error.
	ReadFileAt(ctx context.Context, src schema.RO, root []byte, id K, dst []byte, off int64) (n int, size int64, err error)

	// StatFile returns committed information about a file or directory.
	// exists is false when the identifier is not present in the tree.
	StatFile(ctx context.Context, src schema.RO, root []byte, id K) (info FileInfo, exists bool, err error)

	// ReadDir reads directory entries for the given identifier
	ReadDir(ctx context.Context, src schema.RO, root []byte, id K) ([]DirEntry[K], error)

	// CreateAt creates a new file in the directory
	CreateAt(ctx context.Context, dst schema.WO, src schema.RO, root []byte, parentID K, name string, mode uint32) (K, []byte, error)

	// DeleteAt removes a file from the directory
	DeleteAt(ctx context.Context, dst schema.WO, src schema.RO, root []byte, parentID K, name string) ([]byte, error)
}

// Extent represents a data extent with generic ID
type Extent[K comparable] struct {
	ID    K
	Start int64
	Data  []byte
}

// DirEntry represents a directory entry with generic ID
type DirEntry[K comparable] struct {
	Name  string
	Child K
	Mode  uint32
}

// FileInfo describes a committed filesystem object.
type FileInfo struct {
	Size int64
	Mode uint32
}

var (
	// ErrReadOnly is returned when a write is attempted against a filesystem
	// mounted without write access.
	ErrReadOnly = errors.New("bcfuse: filesystem is mounted read-only")
	// ErrClosed is returned after the filesystem has been shut down.
	ErrClosed = errors.New("bcfuse: filesystem is closed")
)

// Option configures an FS at construction.
type Option[K comparable] func(*FS[K])

// WithReadOnly mounts the filesystem without accepting writes or commits.
func WithReadOnly[K comparable]() Option[K] {
	return func(fs *FS[K]) { fs.readOnly = true }
}

// inodeEntry is the in-memory counterpart of a POSIX inode.
type inodeEntry[K comparable] struct {
	id   K
	mode uint32
}

// FS represents the filesystem with thread-safe operations
type FS[K comparable] struct {
	db     *sqlx.DB
	svc    blobcache.Service
	vol    blobcache.Handle
	scheme Scheme[K]

	readOnly bool

	// rootNode is set the first time FUSERoot is called
	rootNode *Node[K]

	// commitMu serializes volume commits (flush/fsync/release/shutdown and
	// mount-time recovery that performs work).
	commitMu sync.Mutex

	mu sync.Mutex
	// Root bytes held in memory (<1MB)
	root []byte
	// closed is set by Shutdown; afterwards writes are rejected.
	closed bool
	// activeBatch is the batch accepting new writes, or 0 when the current
	// batch has been frozen by an in-flight commit.
	activeBatch int64
	// knownCommitted batches durably committed to the volume whose local
	// confirmation may still be incomplete. They must never be applied again,
	// even if the durable state update has not landed yet.
	knownCommitted map[int64]struct{}
	// Inode management
	nextInode int64
	// Bidirectional mapping between POSIX inodes and scheme identifiers
	inodeToID map[int64]*inodeEntry[K]
	idToInode map[K]int64
}

// New creates a new filesystem instance.
// Init must be called before the filesystem is mounted.
func New[K comparable](db *sqlx.DB, svc blobcache.Service, vol blobcache.Handle, scheme Scheme[K], opts ...Option[K]) *FS[K] {
	fs := &FS[K]{
		db:             db,
		svc:            svc,
		vol:            vol,
		scheme:         scheme,
		nextInode:      2, // Start at 2 since 1 is reserved for root
		knownCommitted: make(map[int64]struct{}),
		inodeToID:      make(map[int64]*inodeEntry[K]),
		idToInode:      make(map[K]int64),
	}
	for _, opt := range opts {
		opt(fs)
	}
	return fs
}

// PutExtent buffers data at startAt for the file identified by id.
// Overlapping writes overwrite earlier bytes; adjacent and overlapping extents
// are coalesced. Sparse (non-adjacent) writes stay separate and read back as
// zero-filled holes. The data is durable in the local database before the
// method returns.
func (fs *FS[K]) PutExtent(ctx context.Context, id K, startAt int64, data []byte) error {
	if startAt < 0 {
		return errors.New("bcfuse: negative write offset")
	}
	if len(data) == 0 {
		return nil
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.closed {
		return ErrClosed
	}
	if fs.readOnly {
		return ErrReadOnly
	}
	if fs.activeBatch == 0 {
		batchID, err := fs.createBatch(ctx)
		if err != nil {
			return err
		}
		fs.activeBatch = batchID
	}
	batchID := fs.activeBatch
	return fs.mergeExtent(ctx, batchID, id, startAt, data)
}

// createBatch allocates a new pending batch. The caller holds fs.mu.
func (fs *FS[K]) createBatch(ctx context.Context) (int64, error) {
	var id int64
	err := sqlutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		return tx.GetContext(ctx, &id,
			`INSERT INTO batches (state, created_at) VALUES (?, ?) RETURNING id`,
			batchPending, time.Now().UnixNano())
	})
	return id, err
}

// mergeExtent folds one write into the buffered extents of a batch/file.
// The caller holds fs.mu, which makes the read-modify-write atomic with
// respect to concurrent writes.
func (fs *FS[K]) mergeExtent(ctx context.Context, batchID int64, id K, startAt int64, data []byte) error {
	return sqlutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		rows, err := tx.QueryxContext(ctx,
			`SELECT start, "end", data FROM extents WHERE batch_id = ? AND id = ? ORDER BY start`,
			batchID, id)
		if err != nil {
			return err
		}
		var existing []seg
		for rows.Next() {
			var s, e int64
			var d []byte
			if err := rows.Scan(&s, &e, &d); err != nil {
				rows.Close()
				return err
			}
			existing = append(existing, seg{start: s, end: e, data: d})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		merged := mergeSegs(existing, seg{start: startAt, end: startAt + int64(len(data)), data: data})

		if _, err := tx.ExecContext(ctx,
			`DELETE FROM extents WHERE batch_id = ? AND id = ?`, batchID, id); err != nil {
			return err
		}
		for _, s := range merged {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO extents (batch_id, id, start, "end", data) VALUES (?, ?, ?, ?, ?)`,
				batchID, id, s.start, s.end, s.data); err != nil {
				return err
			}
		}
		return nil
	})
}

// seg is an in-memory half-open byte interval [start, end).
type seg struct {
	start, end int64
	data       []byte
}

// mergeSegs folds incoming segments over dst. Inputs are ordered by offset and
// later segments win on overlap (the input order is preserved for equal
// starts, so callers pass newer segments after older ones). Adjacent and
// overlapping segments are coalesced; gaps are preserved.
func mergeSegs(dst []seg, incoming ...seg) []seg {
	all := append(append([]seg{}, dst...), incoming...)
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].start < all[j].start
	})
	var out []seg
	for _, s := range all {
		if len(out) == 0 || s.start > out[len(out)-1].end {
			out = append(out, seg{start: s.start, end: s.end, data: append([]byte(nil), s.data...)})
			continue
		}
		last := &out[len(out)-1]
		switch {
		case s.end <= last.end:
			copy(last.data[s.start-last.start:], s.data)
		default:
			buf := make([]byte, s.end-last.start)
			copy(buf, last.data)
			copy(buf[s.start-last.start:], s.data)
			last.data = buf
			last.end = s.end
		}
	}
	return out
}

// GetInode returns the POSIX inode for a scheme identifier
func (fs *FS[K]) GetInode(id K) int64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if ino, exists := fs.idToInode[id]; exists {
		return ino
	}

	return 0
}

// GetOrCreateInode returns the POSIX inode for a scheme identifier, creating one if needed
func (fs *FS[K]) GetOrCreateInode(id K) int64 {
	return fs.getOrCreateInode(id, 0)
}

// touchInode records the mode known from Lookup/Create/Readdir for an inode.
func (fs *FS[K]) touchInode(id K, mode uint32) int64 {
	return fs.getOrCreateInode(id, mode)
}

func (fs *FS[K]) getOrCreateInode(id K, mode uint32) int64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if ino, exists := fs.idToInode[id]; exists {
		if mode != 0 {
			fs.inodeToID[ino].mode = mode
		}
		return ino
	}

	// Create new inode
	ino := fs.nextInode
	fs.nextInode++

	fs.idToInode[id] = ino
	fs.inodeToID[ino] = &inodeEntry[K]{id: id, mode: mode}

	return ino
}

// GetID returns the scheme identifier for a POSIX inode
func (fs *FS[K]) GetID(ino int64) (K, bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	e, exists := fs.inodeToID[ino]
	if !exists {
		var zero K
		return zero, false
	}
	return e.id, true
}

// getInodeMode returns the recorded POSIX mode for an inode, or 0 if unknown.
func (fs *FS[K]) getInodeMode(ino int64) uint32 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if e, ok := fs.inodeToID[ino]; ok {
		return e.mode
	}
	return 0
}

// currentRoot returns a copy of the last committed root.
func (fs *FS[K]) currentRoot() []byte {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]byte(nil), fs.root...)
}

// pendingExtentCount is used by tests and diagnostics.
func (fs *FS[K]) pendingExtentCount(ctx context.Context) (int, error) {
	var n int
	if err := fs.db.GetContext(ctx, &n, `SELECT COUNT(*) FROM extents`); err != nil {
		return 0, err
	}
	return n, nil
}
