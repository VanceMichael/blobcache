package bcfuse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"blobcache.io/blobcache/src/bcsdk"
	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"github.com/jmoiron/sqlx"
)

// Init initializes the database buffer and recovers buffered batches left
// behind by a previous mount or process. It must be called exactly once,
// before the filesystem is mounted.
//
// Recovery policy, driven solely by the durable batch identity:
//   - committed batches are only confirmed locally: their extents are dropped
//     and they are never applied to the volume again.
//   - pending batches with data are retained and committed by the next
//     Flush/fsync/release/unmount.
//
// A read-only mount with pending data is rejected, because the data could not
// be made durable to the volume.
func (fs *FS[K]) Init(ctx context.Context) error {
	fs.commitMu.Lock()
	defer fs.commitMu.Unlock()

	if err := SetupDB(ctx, fs.db); err != nil {
		return fmt.Errorf("bcfuse: initializing buffer database: %w", err)
	}

	// Load the authoritative committed root from the volume.
	tx, err := bcsdk.BeginTx(ctx, fs.svc, fs.vol, blobcache.TxParams{Modify: false})
	if err != nil {
		return fmt.Errorf("bcfuse: loading volume root: %w", err)
	}
	var root []byte
	if err := tx.Load(ctx, &root); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("bcfuse: loading volume root: %w", err)
	}
	if err := tx.Abort(ctx); err != nil {
		return fmt.Errorf("bcfuse: loading volume root: %w", err)
	}

	if err := fs.recoverBatches(ctx, root); err != nil {
		return err
	}
	return nil
}

type batchRow struct {
	ID    int64  `db:"id"`
	State string `db:"state"`
	Root  []byte `db:"root"`
}

// recoverBatches reconciles durable batch state against the volume.
func (fs *FS[K]) recoverBatches(ctx context.Context, volumeRoot []byte) error {
	var batches []batchRow
	if err := fs.db.SelectContext(ctx, &batches,
		`SELECT id, state, root FROM batches ORDER BY id`); err != nil {
		return fmt.Errorf("bcfuse: reading recovery batches: %w", err)
	}

	var pendingIDs []int64
	for _, b := range batches {
		switch b.State {
		case batchCommitted:
			// The volume already contains this batch; finish local confirmation
			// only. The volume root loaded above is authoritative, so it is not
			// rolled back to the stored batch root.
			if err := fs.confirmBatch(ctx, b.ID); err != nil {
				return fmt.Errorf("bcfuse: confirming committed batch %d: %w", b.ID, err)
			}
		case batchPending:
			empty, err := fs.batchEmpty(ctx, b.ID)
			if err != nil {
				return err
			}
			if empty {
				// Nothing was ever accepted into this batch.
				if err := fs.confirmBatch(ctx, b.ID); err != nil {
					return err
				}
				continue
			}
			pendingIDs = append(pendingIDs, b.ID)
		default:
			return fmt.Errorf("bcfuse: batch %d in unknown state %q", b.ID, b.State)
		}
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.root = volumeRoot
	if fs.readOnly && len(pendingIDs) > 0 {
		return fmt.Errorf("bcfuse: %w: %d buffered batch(es) await a read-write mount", ErrReadOnly, len(pendingIDs))
	}
	if len(pendingIDs) > 0 {
		// New writes join the most recent pending batch, preserving batch order.
		fs.activeBatch = pendingIDs[len(pendingIDs)-1]
	} else {
		fs.activeBatch = 0
	}
	return nil
}

// Flush commits all buffered extents to the volume as one ordered set of
// batches, then confirms the batches locally. It is safe to call concurrently
// with writes: the batch boundary is fixed when the commit starts, and writes
// arriving afterwards are accepted into the next batch.
//
// On volume transaction failure the original batches remain pending for
// retry. Once the volume commit succeeds, batches are marked committed before
// their extents are dropped, so a local cleanup failure can never apply the
// same batch twice.
func (fs *FS[K]) Flush(ctx context.Context) error {
	return fs.flush(ctx, false)
}

func (fs *FS[K]) flush(ctx context.Context, allowClosed bool) error {
	fs.commitMu.Lock()
	defer fs.commitMu.Unlock()

	// Finish confirmation of batches whose volume commits already succeeded.
	if err := fs.reconcileCommitted(ctx); err != nil {
		return err
	}

	fs.mu.Lock()
	if fs.closed && !allowClosed {
		fs.mu.Unlock()
		return ErrClosed
	}
	batchIDs, extents, err := fs.snapshotPendingLocked(ctx)
	if err != nil {
		fs.mu.Unlock()
		return err
	}
	if len(batchIDs) == 0 {
		fs.mu.Unlock()
		return nil
	}
	// Freeze the boundary: subsequent writes go to a new batch.
	fs.activeBatch = 0
	// Empty pending batches carry no accepted data (e.g. a write failed after
	// the batch was allocated): drop them locally without a volume tx.
	if len(extents) == 0 {
		fs.mu.Unlock()
		return fs.confirmBatches(ctx, batchIDs)
	}
	fs.mu.Unlock()

	commitErr := fs.commitBatches(ctx, batchIDs, extents)

	fs.mu.Lock()
	if commitErr != nil {
		// Keep the failed batches pending and let new writes rejoin them.
		if fs.activeBatch == 0 && !fs.closed {
			fs.activeBatch = batchIDs[len(batchIDs)-1]
		}
		fs.mu.Unlock()
		return commitErr
	}
	fs.mu.Unlock()

	// Commit succeeded on the volume. Persist the committed identity first,
	// and only then drop the buffered extents.
	if err := fs.markBatchesCommitted(ctx, batchIDs); err != nil {
		// The data is safely on the volume. The durable marker is missing, so
		// remember the identity in-process and surface the error: the caller
		// (fsync/close/unmount) must observe that local confirmation failed.
		fs.mu.Lock()
		for _, id := range batchIDs {
			fs.knownCommitted[id] = struct{}{}
		}
		fs.mu.Unlock()
		return fmt.Errorf("bcfuse: volume commit succeeded but confirmation could not be recorded for batches %v: %w", batchIDs, err)
	}
	if err := fs.confirmBatches(ctx, batchIDs); err != nil {
		// Durably marked committed: next Flush/Init finishes cleanup. The data
		// is committed, so the current caller does not see an I/O failure.
		return nil
	}
	return nil
}

// snapshotPendingLocked returns the ordered pending batch ids and their
// extents, merged per file. Batches known to be committed in-process are
// excluded so they can never be applied twice. The caller holds fs.mu.
func (fs *FS[K]) snapshotPendingLocked(ctx context.Context) ([]int64, []Extent[K], error) {
	var ids []int64
	if err := fs.db.SelectContext(ctx, &ids,
		`SELECT id FROM batches WHERE state = ? ORDER BY id`, batchPending); err != nil {
		return nil, nil, err
	}
	filtered := ids[:0]
	for _, id := range ids {
		if _, known := fs.knownCommitted[id]; !known {
			filtered = append(filtered, id)
		}
	}
	ids = filtered
	if len(ids) == 0 {
		return nil, nil, nil
	}

	args := make([]any, len(ids))
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		placeholders[i] = "?"
	}
	query := `SELECT batch_id, id, start, "end", data FROM extents WHERE batch_id IN (` +
		strings.Join(placeholders, ", ") + `) ORDER BY id, batch_id, start`
	rows, err := fs.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	type rawSeg struct {
		batchID int64
		id      K
		seg
	}
	var raws []rawSeg
	for rows.Next() {
		var batchID int64
		var id K
		var s, e int64
		var d []byte
		if err := rows.Scan(&batchID, &id, &s, &e, &d); err != nil {
			return nil, nil, err
		}
		raws = append(raws, rawSeg{batchID: batchID, id: id, seg: seg{start: s, end: e, data: d}})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(raws) == 0 {
		return ids, nil, nil
	}

	// Merge across all batches in the commit, later batch winning. The scheme
	// receives one coherent overlay per file.
	var extents []Extent[K]
	curID := raws[0].id
	var segs []seg
	flush := func() {
		for _, s := range segs {
			extents = append(extents, Extent[K]{ID: curID, Start: s.start, Data: s.data})
		}
	}
	for _, r := range raws {
		if r.id != curID {
			flush()
			curID = r.id
			segs = nil
		}
		segs = mergeSegs(segs, r.seg)
	}
	flush()
	return ids, extents, nil
}

// commitBatches runs the single volume transaction for a set of batches.
func (fs *FS[K]) commitBatches(ctx context.Context, batchIDs []int64, extents []Extent[K]) error {
	tx, err := bcsdk.BeginTx(ctx, fs.svc, fs.vol, blobcache.TxParams{Modify: true})
	if err != nil {
		return fmt.Errorf("bcfuse: beginning commit transaction for batches %v: %w", batchIDs, err)
	}
	var cur []byte
	if err := tx.Load(ctx, &cur); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("bcfuse: loading root for batches %v: %w", batchIDs, err)
	}
	newRoot, err := fs.scheme.FlushExtents(ctx, tx, tx, cur, extents)
	if err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("bcfuse: applying batches %v: %w", batchIDs, err)
	}
	if err := tx.Save(ctx, newRoot); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("bcfuse: saving root for batches %v: %w", batchIDs, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("bcfuse: committing batches %v to volume: %w", batchIDs, err)
	}

	fs.mu.Lock()
	fs.root = newRoot
	fs.mu.Unlock()
	return nil
}

// reconcileCommitted drives batches known to be committed (durable state or
// in-process knowledge) through local confirmation.
func (fs *FS[K]) reconcileCommitted(ctx context.Context) error {
	// Persist the in-process committed identities; they are idempotent.
	fs.mu.Lock()
	var memoryIDs []int64
	for id := range fs.knownCommitted {
		memoryIDs = append(memoryIDs, id)
	}
	fs.mu.Unlock()
	for _, id := range memoryIDs {
		if err := fs.markBatchCommitted(ctx, id); err != nil {
			return fmt.Errorf("bcfuse: recording committed batch %d: %w", id, err)
		}
	}

	var ids []int64
	if err := fs.db.SelectContext(ctx, &ids,
		`SELECT id FROM batches WHERE state = ? ORDER BY id`, batchCommitted); err != nil {
		return err
	}
	if err := fs.confirmBatches(ctx, ids); err != nil {
		return err
	}
	fs.mu.Lock()
	for _, id := range ids {
		delete(fs.knownCommitted, id)
	}
	fs.mu.Unlock()
	return nil
}

// markBatchesCommitted durably records the volume commit of a batch set.
func (fs *FS[K]) markBatchesCommitted(ctx context.Context, ids []int64) error {
	for _, id := range ids {
		if err := fs.markBatchCommitted(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (fs *FS[K]) markBatchCommitted(ctx context.Context, id int64) error {
	return sqlutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE batches SET state = ?, committed_at = ? WHERE id = ? AND state = ?`,
			batchCommitted, time.Now().UnixNano(), id, batchPending)
		return err
	})
}

// confirmBatches drops the buffered extents and batch records for batches that
// are already committed to the volume. It never applies any data.
func (fs *FS[K]) confirmBatches(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return sqlutil.DoTx(ctx, fs.db, func(tx *sqlx.Tx) error {
		args := make([]any, len(ids))
		placeholders := make([]string, len(ids))
		for i, id := range ids {
			args[i] = id
			placeholders[i] = "?"
		}
		list := strings.Join(placeholders, ", ")
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM extents WHERE batch_id IN (`+list+`)`, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM batches WHERE id IN (`+list+`)`, args...); err != nil {
			return err
		}
		return nil
	})
}

// confirmBatch confirms a single batch.
func (fs *FS[K]) confirmBatch(ctx context.Context, id int64) error {
	return fs.confirmBatches(ctx, []int64{id})
}

func (fs *FS[K]) batchEmpty(ctx context.Context, id int64) (bool, error) {
	var n int
	if err := fs.db.GetContext(ctx, &n,
		`SELECT COUNT(*) FROM extents WHERE batch_id = ?`, id); err != nil {
		return false, err
	}
	return n == 0, nil
}

// Shutdown is the last-attempt path used by unmount. When buffered data
// exists it performs one final Flush bounded by ctx: a deadline or canceled
// context makes the attempt terminate in bounded time and the error is
// returned to the caller instead of silently dropping accepted writes.
// Accepted writes are always recoverable afterwards, because they remain
// durable pending batches until the volume commit is confirmed.
func (fs *FS[K]) Shutdown(ctx context.Context) error {
	fs.mu.Lock()
	if fs.closed {
		fs.mu.Unlock()
		return nil
	}
	fs.closed = true
	fs.mu.Unlock()

	if err := fs.flush(ctx, true); err != nil {
		return err
	}
	return nil
}
