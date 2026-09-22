package scheme_glfs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"

	"blobcache.io/blobcache/src/schema"
	"blobcache.io/blobcache/src/schema/bcfuse"
	"blobcache.io/glfs"
	"go.brendoncarroll.net/exp/streams"
)

// Verify that Scheme implements bcfs.Scheme[string]
var _ bcfuse.Scheme[string] = (*Scheme)(nil)

// Scheme implements the bcfs.Scheme interface using GLFS
type Scheme struct {
	Machine *glfs.Machine
}

// NewScheme creates a new GLFS scheme
func NewScheme() *Scheme {
	return &Scheme{Machine: glfs.NewMachine()}
}

// maxSizer is implemented by bcsdk.Tx and bounds whole-blob reads.
type maxSizer interface {
	MaxSize() int
}

// fallbackMaxSize is used when the destination does not expose a MaxSize.
const fallbackMaxSize = 1 << 30

// FlushExtents writes all the extents to the volume.
// Extents for the same file identifier are sorted by offset and do not
// overlap; existing file content is read and overwritten at those offsets,
// with sparse writes zero-extending the file.
func (s *Scheme) FlushExtents(ctx context.Context, dst schema.WO, src schema.RO, root []byte, extents []bcfuse.Extent[string]) ([]byte, error) {
	// Load the current filesystem state
	var fsRef glfs.Ref
	if len(root) > 0 {
		if err := json.Unmarshal(root, &fsRef); err != nil {
			return nil, fmt.Errorf("failed to unmarshal root: %w", err)
		}
	} else {
		// Create empty filesystem
		ref, err := glfs.PostTreeSlice(ctx, dst, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create empty tree: %w", err)
		}
		fsRef = *ref
	}

	maxSize := fallbackMaxSize
	if mx, ok := dst.(maxSizer); ok {
		maxSize = mx.MaxSize()
	}

	// Preserve file order while grouping ranges by file.
	var ids []string
	byID := make(map[string][]bcfuse.Extent[string])
	for _, e := range extents {
		if _, ok := byID[e.ID]; !ok {
			ids = append(ids, e.ID)
		}
		byID[e.ID] = append(byID[e.ID], e)
	}

	entries := make([]glfs.TreeEntry, 0, len(ids))
	for _, id := range ids {
		es := byID[id]

		// Read current content and preserve the recorded mode, if the file exists.
		var cur []byte
		mode := os.FileMode(0o644)
		fileRef, err := glfs.GetAtPath(ctx, src, fsRef, id)
		switch {
		case err == nil:
			if fileRef.Type == glfs.TypeTree {
				return nil, fmt.Errorf("%s is a directory, cannot write extents to it", id)
			}
			b, err := glfs.GetBlobBytes(ctx, src, *fileRef, maxSize)
			if err != nil {
				return nil, fmt.Errorf("failed to read file %s: %w", id, err)
			}
			cur = b
			if m, err := s.lookupMode(ctx, src, fsRef, id); err == nil && m != 0 {
				mode = m
			}
		case glfs.IsErrNoEnt(err):
			// New file: nothing to splice onto.
		default:
			return nil, fmt.Errorf("failed to look up %s: %w", id, err)
		}

		var end int64
		for _, e := range es {
			if e.Start+int64(len(e.Data)) > end {
				end = e.Start + int64(len(e.Data))
			}
		}
		if int(end) < len(cur) {
			end = int64(len(cur))
		}
		buf := make([]byte, end)
		copy(buf, cur)
		for _, e := range es {
			copy(buf[e.Start:], e.Data)
		}

		// Create a blob for the materialized file.
		newRef, err := glfs.PostBlob(ctx, dst, bytes.NewReader(buf))
		if err != nil {
			return nil, fmt.Errorf("failed to post blob for %s: %w", id, err)
		}
		entries = append(entries, glfs.TreeEntry{
			Name:     id,
			FileMode: mode,
			Ref:      *newRef,
		})
	}

	// Create one tree with all files and merge it with the current filesystem.
	treeRef, err := glfs.PostTreeSlice(ctx, dst, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to create tree: %w", err)
	}
	mergedRef, err := glfs.Merge(ctx, dst, src, fsRef, *treeRef)
	if err != nil {
		return nil, fmt.Errorf("failed to merge extents: %w", err)
	}

	newRoot, err := json.Marshal(mergedRef)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal new root: %w", err)
	}
	return newRoot, nil
}

// lookupMode returns the recorded POSIX mode for an existing tree entry.
func (s *Scheme) lookupMode(ctx context.Context, src schema.RO, fsRef glfs.Ref, id string) (os.FileMode, error) {
	dir, base := path.Split(id)
	dirRef := &fsRef
	if dir != "" {
		r, err := glfs.GetAtPath(ctx, src, fsRef, path.Clean(dir))
		if err != nil {
			return 0, err
		}
		dirRef = r
	}
	tr, err := s.Machine.NewTreeReader(src, *dirRef)
	if err != nil {
		return 0, err
	}
	var mode os.FileMode
	err = streams.ForEach(ctx, tr, func(e glfs.TreeEntry) error {
		if e.Name == base {
			mode = e.FileMode
			return errStop
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		return 0, err
	}
	return mode, nil
}

var errStop = errors.New("stop")

// ReadFileAt reads up to len(dst) bytes at off and reports the file size.
func (s *Scheme) ReadFileAt(ctx context.Context, src schema.RO, root []byte, id string, dst []byte, off int64) (int, int64, error) {
	if len(root) == 0 {
		return 0, 0, nil
	}
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return 0, 0, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	fileRef, err := glfs.GetAtPath(ctx, src, fsRef, id)
	if err != nil {
		if glfs.IsErrNoEnt(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to get file %s: %w", id, err)
	}
	if fileRef.Type == glfs.TypeTree {
		return 0, 0, fmt.Errorf("%s is a directory: %w", id, syscall.EISDIR)
	}
	size := int64(fileRef.Size)
	if off >= size {
		return 0, size, nil
	}

	r, err := glfs.GetBlob(ctx, src, *fileRef)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file %s: %w", id, err)
	}
	n, err := r.ReadAt(dst, off)
	if err != nil && !(errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return n, size, fmt.Errorf("failed to read file %s: %w", id, err)
	}
	return n, size, nil
}

// StatFile returns size and mode for a committed object.
func (s *Scheme) StatFile(ctx context.Context, src schema.RO, root []byte, id string) (bcfuse.FileInfo, bool, error) {
	if len(root) == 0 {
		return bcfuse.FileInfo{}, false, nil
	}
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return bcfuse.FileInfo{}, false, fmt.Errorf("failed to unmarshal root: %w", err)
	}
	r, err := glfs.GetAtPath(ctx, src, fsRef, id)
	if err != nil {
		if glfs.IsErrNoEnt(err) {
			return bcfuse.FileInfo{}, false, nil
		}
		return bcfuse.FileInfo{}, false, fmt.Errorf("failed to stat %s: %w", id, err)
	}
	if r.Type == glfs.TypeTree {
		return bcfuse.FileInfo{Size: int64(r.Size), Mode: 0o040000 | 0o755}, true, nil
	}
	mode, _ := s.lookupMode(ctx, src, fsRef, id)
	if mode == 0 {
		mode = 0o644
	}
	return bcfuse.FileInfo{Size: int64(r.Size), Mode: 0o100000 | uint32(mode.Perm())}, true, nil
}

// ReadDir reads directory entries for the given identifier
func (s *Scheme) ReadDir(ctx context.Context, src schema.RO, root []byte, id string) ([]bcfuse.DirEntry[string], error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return nil, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	// Get the directory reference
	var dirRef *glfs.Ref
	var err error
	if id == "" || id == "/" {
		dirRef = &fsRef
	} else {
		dirRef, err = glfs.GetAtPath(ctx, src, fsRef, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get directory %s: %w", id, err)
		}
	}

	if dirRef.Type != glfs.TypeTree {
		return nil, fmt.Errorf("path %s is not a directory", id)
	}

	// Read directory entries
	tr, err := s.Machine.NewTreeReader(src, *dirRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create tree reader: %w", err)
	}

	var result []bcfuse.DirEntry[string]
	err = streams.ForEach(ctx, tr, func(entry glfs.TreeEntry) error {
		// Convert file mode to POSIX mode
		mode := uint32(entry.FileMode)
		if entry.Ref.Type == glfs.TypeTree {
			mode = mode | 0040000 // S_IFDIR
		} else {
			mode = mode | 0100000 // S_IFREG
		}

		childPath := entry.Name
		if id != "" && id != "/" {
			childPath = id + "/" + entry.Name
		}

		result = append(result, bcfuse.DirEntry[string]{
			Name:  entry.Name,
			Child: childPath,
			Mode:  mode,
		})
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read directory entries: %w", err)
	}

	return result, nil
}

// CreateAt creates a new file in the directory
func (s *Scheme) CreateAt(ctx context.Context, dst schema.WO, src schema.RO, root []byte, parentID string, name string, mode uint32) (string, []byte, error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if len(root) > 0 {
		if err := json.Unmarshal(root, &fsRef); err != nil {
			return "", nil, fmt.Errorf("failed to unmarshal root: %w", err)
		}
	} else {
		// Create empty filesystem
		ref, err := glfs.PostTreeSlice(ctx, dst, nil)
		if err != nil {
			return "", nil, fmt.Errorf("failed to create empty tree: %w", err)
		}
		fsRef = *ref
	}

	// Create empty file
	fileRef, err := glfs.PostBlob(ctx, dst, bytes.NewReader([]byte{}))
	if err != nil {
		return "", nil, fmt.Errorf("failed to create empty file: %w", err)
	}

	// Create the file path
	filePath := name
	if parentID != "" && parentID != "/" {
		filePath = parentID + "/" + name
	}

	// Create a tree entry for this file
	entry := glfs.TreeEntry{
		Name:     name,
		FileMode: os.FileMode(mode),
		Ref:      *fileRef,
	}

	// Create a new tree with this file
	treeRef, err := glfs.PostTreeSlice(ctx, dst, []glfs.TreeEntry{entry})
	if err != nil {
		return "", nil, fmt.Errorf("failed to create tree: %w", err)
	}

	// Merge with the existing filesystem
	newFsRef, err := glfs.Merge(ctx, dst, src, fsRef, *treeRef)
	if err != nil {
		return "", nil, fmt.Errorf("failed to merge new file: %w", err)
	}

	// Marshal the new root
	newRoot, err := json.Marshal(newFsRef)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal new root: %w", err)
	}

	return filePath, newRoot, nil
}

// DeleteAt removes a file from the directory
func (s *Scheme) DeleteAt(ctx context.Context, dst schema.WO, src schema.RO, root []byte, parentID string, name string) ([]byte, error) {
	// Load the filesystem
	var fsRef glfs.Ref
	if err := json.Unmarshal(root, &fsRef); err != nil {
		return nil, fmt.Errorf("failed to unmarshal root: %w", err)
	}

	// For now, return an error since GLFS doesn't have a simple delete operation
	// This would need to be implemented by reconstructing the tree without the target file
	return nil, fmt.Errorf("delete operation not yet implemented")
}
