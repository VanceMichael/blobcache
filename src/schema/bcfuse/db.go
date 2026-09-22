package bcfuse

import (
	"context"

	"blobcache.io/blobcache/src/internal/migrations"
	"blobcache.io/blobcache/src/internal/sqlutil"
	"github.com/jmoiron/sqlx"
)

func SetupDB(ctx context.Context, db *sqlx.DB) error {
	return sqlutil.DoTx(ctx, db, func(tx *sqlx.Tx) error {
		return migrations.EnsureAll(tx, migs)
	})
}

// Batch states.
//
// A batch is the durable identity of one volume commit of buffered extents.
//   - batchPending: extents are buffered locally and have not been confirmed as
//     committed to the volume. Recovery must (re)try the volume transaction.
//   - batchCommitted: the volume transaction has returned success. The resulting
//     root is recorded with the batch. Recovery must only finish the local
//     confirmation (drop the extents); it must never apply the batch again.
const (
	batchPending   = "pending"
	batchCommitted = "committed"
)

var migs = []migrations.Migration{
	{
		RowID: 1,
		Name:  "create_extents_table",
		SQLText: `
		CREATE TABLE extents (
			id TEXT NOT NULL,
			start INTEGER NOT NULL,
			"end" INTEGER NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY (id, start)
		) WITHOUT ROWID, STRICT`,
	},
	{
		// batches is the recoverable write-ahead log of extent buffer commits.
		RowID: 2,
		Name:  "create_batches_table",
		SQLText: `
		CREATE TABLE batches (
			id INTEGER PRIMARY KEY,
			state TEXT NOT NULL,
			root BLOB,
			created_at INTEGER NOT NULL,
			committed_at INTEGER
		) STRICT`,
	},
	{
		// Rebuild the extents table with a batch_id, so that committing a batch
		// can only confirm the extents that belonged to the frozen batch
		// boundary; concurrent writes land in later batches.
		RowID:   3,
		Name:    "rename_extents_legacy",
		SQLText: `ALTER TABLE extents RENAME TO extents_legacy_v1`,
	},
	{
		RowID: 4,
		Name:  "create_extents_table_v2",
		SQLText: `
		CREATE TABLE extents (
			batch_id INTEGER NOT NULL,
			id TEXT NOT NULL,
			start INTEGER NOT NULL,
			"end" INTEGER NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY (batch_id, id, start)
		) WITHOUT ROWID, STRICT`,
	},
	{
		// Only materialize the recovery batch when there is buffered data to
		// migrate, so fresh mounts do not accumulate empty batches.
		RowID: 5,
		Name:  "seed_legacy_batch",
		SQLText: `
		INSERT INTO batches (id, state, created_at)
		SELECT 1, 'pending', 0
		WHERE EXISTS (SELECT 1 FROM extents_legacy_v1)`,
	},
	{
		RowID:   6,
		Name:    "migrate_legacy_extents",
		SQLText: `INSERT INTO extents (batch_id, id, start, "end", data) SELECT 1, id, start, "end", data FROM extents_legacy_v1`,
	},
	{
		RowID:   7,
		Name:    "drop_extents_legacy",
		SQLText: `DROP TABLE extents_legacy_v1`,
	},
}
