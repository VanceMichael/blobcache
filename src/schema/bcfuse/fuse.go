package bcfuse

import (
	"context"
	"errors"
	"io"
	"syscall"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// FUSERoot returns the root node for FUSE mounting
func (fsx *FS[K]) FUSERoot() *Node[K] {
	if fsx.rootNode == nil {
		fsx.rootNode = &Node[K]{
			fs:  fsx,
			ino: 1, // Root directory inode is always 1
		}
	}
	return fsx.rootNode
}

// Node represents a FUSE node with generic filesystem reference
type Node[K comparable] struct {
	fs.Inode
	fs  *FS[K]
	ino int64
}

var _ fs.NodeGetattrer = (*Node[string])(nil)
var _ fs.NodeLookuper = (*Node[string])(nil)
var _ fs.NodeReaddirer = (*Node[string])(nil)
var _ fs.NodeCreater = (*Node[string])(nil)
var _ fs.NodeUnlinker = (*Node[string])(nil)
var _ fs.NodeReader = (*Node[string])(nil)
var _ fs.NodeWriter = (*Node[string])(nil)
var _ fs.NodeFlusher = (*Node[string])(nil)
var _ fs.NodeFsyncer = (*Node[string])(nil)
var _ fs.NodeReleaser = (*Node[string])(nil)

const (
	modeDirDefault  = fuse.S_IFDIR | 0755
	modeFileDefault = fuse.S_IFREG | 0644
)

// Getattr returns file attributes
func (n *Node[K]) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// Root directory is always known.
	if n.ino == 1 {
		out.Attr.Ino = 1
		out.Attr.Mode = modeDirDefault
		out.Attr.Nlink = 2
		return 0
	}

	id, exists := n.fs.GetID(n.ino)
	if !exists {
		return syscall.ENOENT
	}

	mode := n.fs.getInodeMode(n.ino)

	// Begin read transaction
	tx, err := bcsdk.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	info, present, err := n.fs.scheme.StatFile(ctx, tx, n.fs.currentRoot(), id)
	if err != nil {
		return syscall.EIO
	}
	if !present {
		// A buffered file not yet committed: size comes from the buffer.
		size, serr := n.fs.FileSize(ctx, id)
		if serr != nil {
			return syscall.EIO
		}
		if size == 0 {
			return syscall.ENOENT
		}
		out.Attr.Ino = uint64(n.ino)
		out.Attr.Mode = firstMode(mode, modeFileDefault)
		out.Attr.Nlink = 1
		out.Attr.Size = uint64(size)
		return 0
	}

	if mode == 0 {
		mode = info.Mode
	}
	out.Attr.Ino = uint64(n.ino)
	out.Attr.Mode = mode
	out.Attr.Nlink = 1
	if mode&fuse.S_IFDIR != 0 {
		out.Attr.Nlink = 2
	} else {
		size, serr := n.fs.FileSize(ctx, id)
		if serr != nil {
			return syscall.EIO
		}
		out.Attr.Size = uint64(size)
	}
	return 0
}

func firstMode(mode, fallback uint32) uint32 {
	if mode != 0 {
		return mode
	}
	return fallback
}

// Lookup finds a child node by name
func (n *Node[K]) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin read transaction
	tx, err := bcsdk.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Read directory entries from the scheme
	entries, err := n.fs.scheme.ReadDir(ctx, tx, n.fs.currentRoot(), id)
	if err != nil {
		return nil, syscall.EIO
	}

	// Find the requested entry
	for _, entry := range entries {
		if entry.Name == name {
			// Get or create inode for this child
			childIno := n.fs.touchInode(entry.Child, entry.Mode)

			// Set attributes for the entry
			out.Attr.Ino = uint64(childIno)
			out.Attr.Mode = entry.Mode
			out.Attr.Nlink = 1

			// Create child node
			child := &Node[K]{
				fs:  n.fs,
				ino: childIno,
			}

			return n.NewInode(ctx, child, fs.StableAttr{
				Mode: entry.Mode,
				Ino:  uint64(childIno),
			}), 0
		}
	}

	return nil, syscall.ENOENT
}

// Readdir reads directory entries
func (n *Node[K]) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin read transaction
	tx, err := bcsdk.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return nil, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Read directory entries from the scheme
	entries, err := n.fs.scheme.ReadDir(ctx, tx, n.fs.currentRoot(), id)
	if err != nil {
		return nil, syscall.EIO
	}

	// Convert to FUSE directory entries
	var fuseEntries []fuse.DirEntry
	for _, entry := range entries {
		childIno := n.fs.touchInode(entry.Child, entry.Mode)
		fuseEntries = append(fuseEntries, fuse.DirEntry{
			Name: entry.Name,
			Ino:  uint64(childIno),
			Mode: entry.Mode,
		})
	}

	return fs.NewListDirStream(fuseEntries), 0
}

// Create creates a new file
func (n *Node[K]) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if n.fs.readOnly {
		return nil, nil, 0, syscall.EROFS
	}
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return nil, nil, 0, syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin write transaction
	tx, err := bcsdk.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Modify: true})
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Create the file in the scheme
	childID, newRoot, err := n.fs.scheme.CreateAt(ctx, tx, tx, n.fs.currentRoot(), id, name, mode)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	// Save and Commit the transaction
	if err := tx.Save(ctx, newRoot); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, 0, syscall.EIO
	}

	// Update the root
	n.fs.mu.Lock()
	n.fs.root = newRoot
	n.fs.mu.Unlock()

	// Create inode for the new file
	if mode == 0 {
		mode = modeFileDefault
	}
	childIno := n.fs.touchInode(childID, mode)

	// Set attributes
	out.Attr.Ino = uint64(childIno)
	out.Attr.Mode = mode
	out.Attr.Nlink = 1

	// Create child node
	child := &Node[K]{
		fs:  n.fs,
		ino: childIno,
	}

	inode := n.NewInode(ctx, child, fs.StableAttr{
		Mode: mode,
		Ino:  uint64(childIno),
	})

	return inode, nil, 0, 0
}

// Unlink removes a file
func (n *Node[K]) Unlink(ctx context.Context, name string) syscall.Errno {
	if n.fs.readOnly {
		return syscall.EROFS
	}
	// Get the scheme ID for this directory
	id, exists := n.fs.GetID(n.ino)
	if !exists && n.ino != 1 {
		return syscall.ENOENT
	}

	// For root directory, use zero value of K
	if n.ino == 1 {
		var zeroK K
		id = zeroK
	}

	// Begin write transaction
	tx, err := bcsdk.BeginTx(ctx, n.fs.svc, n.fs.vol, blobcache.TxParams{Modify: true})
	if err != nil {
		return syscall.EIO
	}
	defer func() { _ = tx.Abort(ctx) }()

	// Delete the file from the scheme
	newRoot, err := n.fs.scheme.DeleteAt(ctx, tx, tx, n.fs.currentRoot(), id, name)
	if err != nil {
		return syscall.EIO
	}

	// Save and Commit the transaction
	if err := tx.Save(ctx, newRoot); err != nil {
		return syscall.EIO
	}
	if err := tx.Commit(ctx); err != nil {
		return syscall.EIO
	}

	// Update the root
	n.fs.mu.Lock()
	n.fs.root = newRoot
	n.fs.mu.Unlock()

	return 0
}

// Read reads file data, overlaying buffered uncommitted extents.
func (n *Node[K]) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// Get the scheme ID for this file
	id, exists := n.fs.GetID(n.ino)
	if !exists {
		return nil, syscall.ENOENT
	}

	buf := make([]byte, len(dest))
	n2, err := n.fs.ReadFile(ctx, id, buf, off)
	if err != nil && !(errors.Is(err, io.EOF) && n2 == 0) {
		return nil, syscall.EIO
	}
	if n2 == 0 {
		return fuse.ReadResultData([]byte{}), 0
	}
	return fuse.ReadResultData(buf[:n2]), 0
}

// Write writes file data into the extent buffer.
func (n *Node[K]) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (written uint32, errno syscall.Errno) {
	// Get the scheme ID for this file
	id, exists := n.fs.GetID(n.ino)
	if !exists {
		return 0, syscall.ENOENT
	}

	// Write to the extent buffer
	err := n.fs.PutExtent(ctx, id, off, data)
	if err != nil {
		return 0, errnoForWrite(err)
	}

	return uint32(len(data)), 0
}

// Flush is invoked on close(2) of a file descriptor. Buffered extents are
// committed to the volume and only confirmed once the commit succeeds.
func (n *Node[K]) Flush(ctx context.Context, f fs.FileHandle) syscall.Errno {
	return commitErrno(n.fs.Flush(ctx))
}

// Fsync commits buffered extents to the volume.
func (n *Node[K]) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	return commitErrno(n.fs.Flush(ctx))
}

// Release is invoked when the last reference to an open file is dropped and
// performs the same commit as fsync/close.
func (n *Node[K]) Release(ctx context.Context, f fs.FileHandle) syscall.Errno {
	return commitErrno(n.fs.Flush(ctx))
}

func errnoForWrite(err error) syscall.Errno {
	switch {
	case errors.Is(err, ErrReadOnly):
		return syscall.EROFS
	case errors.Is(err, ErrClosed):
		return syscall.EIO
	default:
		return syscall.EIO
	}
}

func commitErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if errors.Is(err, ErrReadOnly) {
		return syscall.EROFS
	}
	return syscall.EIO
}
