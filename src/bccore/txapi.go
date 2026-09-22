package bccore

import (
	"context"
	"errors"
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.uber.org/zap"
)

// This next section implements blobcache.TxAPI
var _ blobcache.TxAPI = &System{}

// enterOp registers an in-flight operation on the transaction.
// Operations are refused once the transaction is sealed or done.
// The returned function must be called when the operation exits.
func (tx *transaction) enterOp() (func(), error) {
	tx.opsMu.RLock()
	if tx.sealed || tx.done {
		tx.opsMu.RUnlock()
		return nil, blobcache.ErrTxDone{}
	}
	tx.opsWG.Add(1)
	tx.opsMu.RUnlock()
	return tx.opsWG.Done, nil
}

// seal refuses new operations and waits for the operations already inside
// the transaction to exit. It is idempotent.
func (tx *transaction) seal() {
	tx.opsMu.Lock()
	tx.sealed = true
	tx.opsMu.Unlock()
	tx.opsWG.Wait()
}

// unseal reopens the transaction for operations after a terminal backend
// call failed and the transaction remains usable.
func (tx *transaction) unseal() {
	tx.opsMu.Lock()
	tx.sealed = false
	tx.opsMu.Unlock()
}

// commit drives the transaction to its committed terminal state.
// commit serializes against abort and the reaper terminate, so the
// transaction ends in exactly one terminal state. A failed backend Commit
// leaves the transaction usable, preserving the previous retry behavior.
func (tx *transaction) commit(ctx context.Context) error {
	tx.termMu.Lock()
	defer tx.termMu.Unlock()
	if tx.done {
		return blobcache.ErrTxDone{}
	}
	tx.seal()
	if err := tx.backend.Commit(ctx); err != nil {
		tx.unseal()
		return err
	}
	tx.done = true
	tx.committed = true
	return nil
}

// abort drives the transaction to its aborted terminal state in response
// to an explicit Abort call. A failed backend Abort leaves the transaction
// sealed=false and usable, preserving the previous retry behavior.
func (tx *transaction) abort(ctx context.Context) error {
	tx.termMu.Lock()
	defer tx.termMu.Unlock()
	if tx.done {
		// Abort after Commit/Abort is a no-op in every backend.
		return nil
	}
	tx.seal()
	if err := tx.backend.Abort(ctx); err != nil {
		tx.unseal()
		return err
	}
	tx.done = true
	return nil
}

// terminate is the reaper side of the termination flow, shared by
// handle expiry in Cleanup, the last handle Drop, and Close/AbortAll.
// It seals the transaction, waits for in-flight operations to exit, and
// then aborts the backend. If the backend Abort fails the transaction
// stays sealed and present, so that a later Cleanup or Close can retry;
// a transaction which reached the committed state is never rolled back.
func (tx *transaction) terminate(ctx context.Context) error {
	tx.seal()
	tx.termMu.Lock()
	defer tx.termMu.Unlock()
	if tx.done {
		return nil
	}
	if err := tx.backend.Abort(ctx); err != nil {
		return err
	}
	tx.done = true
	return nil
}

// finishAfterTerminal drops the handle used to end the transaction and,
// if it was the last handle, removes the core transaction state now that
// the backend has confirmed release.
func (sys *System) finishAfterTerminal(h blobcache.Handle) {
	sys.mu.Lock()
	defer sys.mu.Unlock()
	sys.handles.Drop(h)
	if !sys.handles.isAlive(h.OID) {
		delete(sys.txns, h.OID)
	}
}

func (sys *System) InspectTx(ctx context.Context, txh blobcache.Handle) (*blobcache.TxInfo, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "InspectTx"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "InspectTx"), zap.Stringer("oid", txh.OID))
	tx, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_INSPECT)
	if err != nil {
		return nil, err
	}
	defer release()
	params := tx.backend.Params()
	return &blobcache.TxInfo{
		ID:       txh.OID,
		Volume:   tx.volume.info.ID,
		MaxSize:  int64(tx.backend.MaxSize()),
		HashAlgo: tx.backend.HashAlgo(),
		Params:   params,
	}, nil
}

func (sys *System) Commit(ctx context.Context, txh blobcache.Handle) error {
	logctx.Info(ctx, "begin", zap.String("method", "Commit"), zap.Stringer("oid", txh.OID))
	defer logctx.Info(ctx, "done", zap.String("method", "Commit"), zap.Stringer("oid", txh.OID))
	tx, release, err := sys.resolveTx(txh, true, 0)
	if err != nil {
		return err
	}
	release()
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COMMIT"}
	}
	if err := tx.commit(ctx); err != nil {
		return setErrTxOID(err, txh.OID)
	}
	sys.finishAfterTerminal(txh)
	sys.hub.Publish(ctx, tx.volume.info.ID, tx.volume)
	return nil
}

func (sys *System) Abort(ctx context.Context, txh blobcache.Handle) error {
	logctx.Info(ctx, "begin", zap.String("method", "Abort"), zap.Stringer("oid", txh.OID))
	defer logctx.Info(ctx, "done", zap.String("method", "Abort"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, false, 0)
	if err != nil {
		return err
	}
	release()
	if err := txn.abort(ctx); err != nil {
		return setErrTxOID(err, txh.OID)
	}
	sys.finishAfterTerminal(txh)
	return nil
}

func (sys *System) Load(ctx context.Context, txh blobcache.Handle, dst *[]byte) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Load"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Load"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_LOAD)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.Load(ctx, dst), txh.OID)
}

func (sys *System) Save(ctx context.Context, txh blobcache.Handle, root []byte) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Save"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Save"), zap.Stringer("oid", txh.OID))
	tx, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_SAVE)
	if err != nil {
		return err
	}
	defer release()
	if p := tx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "SAVE"}
	}
	if sys.p.OnSave != nil {
		if err := sys.p.OnSave(ctx, tx.volume.backend, tx.backend, root); err != nil {
			return err
		}
	}
	return setErrTxOID(tx.backend.Save(ctx, root), txh.OID)
}

func (sys *System) Post(ctx context.Context, txh blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Post"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Post"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_POST)
	if err != nil {
		return blobcache.CID{}, err
	}
	defer release()
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.CID{}, blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "POST"}
	}
	cid, err := txn.backend.Post(ctx, data, opts)
	if err != nil {
		return blobcache.CID{}, setErrTxOID(err, txh.OID)
	}
	return cid, nil
}

func (sys *System) Exists(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst *blobcache.BitMap) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Exists"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Exists"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_EXISTS)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.Exists(ctx, cids, dst), txh.OID)
}

func (sys *System) Get(ctx context.Context, txh blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Get"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Get"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_GET)
	if err != nil {
		return 0, err
	}
	defer release()
	n, err := txn.backend.Get(ctx, cid, buf, opts)
	if err != nil {
		return 0, setErrTxOID(err, txh.OID)
	}
	if !opts.SkipVerify {
		cid2 := txn.backend.HashAlgo().KeyedHash(opts.Salt, buf[:n])
		if cid2 != cid {
			return -1, blobcache.ErrBadData{
				Salt:     opts.Salt,
				Expected: cid,
				Actual:   cid2,
				Len:      n,
			}
		}
	}
	return n, nil
}

func (sys *System) Delete(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Delete"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Delete"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_DELETE)
	if err != nil {
		return err
	}
	defer release()
	if p := txn.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "DELETE"}
	}
	return setErrTxOID(txn.backend.Delete(ctx, cids), txh.OID)
}

func (sys *System) Copy(ctx context.Context, txh blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, out *blobcache.BitMap) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Copy"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Copy"), zap.Stringer("oid", txh.OID))
	dstTx, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_COPY_TO)
	if err != nil {
		return err
	}
	defer release()
	if p := dstTx.backend.Params(); !p.Modify {
		return blobcache.ErrTxReadOnly{Tx: txh.OID, Op: "COPY"}
	}

	type srcTxn struct {
		oid blobcache.OID
		tx  *transaction
	}
	resolvedSrcs := make([]srcTxn, len(srcTxns))
	for i, srcH := range srcTxns {
		src, srcRelease, err := sys.resolveTx(srcH, true, blobcache.Action_TX_COPY_FROM)
		if err != nil {
			return err
		}
		resolvedSrcs[i] = srcTxn{oid: srcH.OID, tx: src}
		// The source transactions stay entered for the duration of the Copy:
		// the reaper waits for this operation to exit before aborting them.
		defer srcRelease()
	}

	var buf []byte
	var exists blobcache.BitMap
	for i, cid := range cids {
		if len(resolvedSrcs) == 0 {
			continue
		}
		start := int(cid[0]) % len(resolvedSrcs)
		for j := range resolvedSrcs {
			src := resolvedSrcs[(start+j)%len(resolvedSrcs)]
			exists = exists[:0]
			if err := src.tx.backend.Exists(ctx, []blobcache.CID{cid}, &exists); err != nil {
				return fmt.Errorf("copy from tx %v: %w", src.oid, err)
			}
			if !exists.IsSet(0) {
				continue
			}
			srcMax := src.tx.backend.MaxSize()
			if cap(buf) < srcMax {
				buf = make([]byte, srcMax)
			}
			n, err := src.tx.backend.Get(ctx, cid, buf[:srcMax], blobcache.GetOpts{})
			if err != nil {
				if blobcache.IsErrNotFound(err) {
					continue
				}
				return fmt.Errorf("copy from tx %v: %w", src.oid, err)
			}
			data := buf[:n]
			if dstTx.backend.HashAlgo().Hash(data) != cid {
				continue
			}
			cid2, err := dstTx.backend.Post(ctx, data, blobcache.PostOpts{})
			if err != nil {
				var eTooLarge blobcache.ErrTooLarge
				if errors.As(err, &eTooLarge) {
					continue
				}
				return setErrTxOID(err, txh.OID)
			}
			if cid2 != cid {
				continue
			}
			out.Set(i)
			break
		}
	}
	return nil
}

func (sys *System) Visit(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Visit"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Visit"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_VISIT)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.Visit(ctx, cids), txh.OID)
}

func (sys *System) IsVisited(ctx context.Context, txh blobcache.Handle, cids []blobcache.CID, dst *blobcache.BitMap) error {
	logctx.Debug(ctx, "begin", zap.String("method", "IsVisited"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "IsVisited"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_IS_VISITED)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.IsVisited(ctx, cids, dst), txh.OID)
}

func (sys *System) Link(ctx context.Context, txh blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	logctx.Debug(ctx, "begin", zap.String("method", "Link"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Link"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_LINK_FROM)
	if err != nil {
		return nil, err
	}
	defer release()
	volTo, rights, err := sys.resolveVol(target)
	if err != nil {
		return nil, err
	}
	if err := sys.p.OnLink(ctx, blobcache.Info{Volume: &volTo.info}, volTo.backend); err != nil {
		return nil, err
	}
	linkRights := rights.Share() & mask
	ltok, err := txn.backend.Link(ctx, volTo.info.ID, linkRights, volTo.backend)
	if err != nil {
		return nil, setErrTxOID(err, txh.OID)
	}
	return ltok, nil
}

func (sys *System) Unlink(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "Unlink"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "Unlink"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_UNLINK_FROM)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.Unlink(ctx, targets), txh.OID)
}

func (sys *System) VisitLinks(ctx context.Context, txh blobcache.Handle, targets []blobcache.LinkID) error {
	logctx.Debug(ctx, "begin", zap.String("method", "VisitLinks"), zap.Stringer("oid", txh.OID))
	defer logctx.Debug(ctx, "done", zap.String("method", "VisitLinks"), zap.Stringer("oid", txh.OID))
	txn, release, err := sys.resolveTx(txh, true, blobcache.Action_TX_VISIT_LINKS)
	if err != nil {
		return err
	}
	defer release()
	return setErrTxOID(txn.backend.VisitLinks(ctx, targets), txh.OID)
}

// AbortAll terminates all transactions through the same convergent
// termination flow used by Drop and Cleanup. Handles are dropped first,
// in-flight operations are drained, and each backend is aborted.
// A failing Abort is logged and retained for a later Cleanup retry, but it
// does not prevent the other transactions from being terminated; shutdown
// itself is best effort and always returns nil.
func (s *System) AbortAll(ctx context.Context) error {
	s.mu.Lock()
	txns := make(map[blobcache.OID]*transaction, len(s.txns))
	for oid, txn := range s.txns {
		txns[oid] = txn
		s.handles.DropAllForOID(oid)
	}
	s.mu.Unlock()

	for oid, txn := range txns {
		if err := txn.terminate(ctx); err != nil {
			logctx.Warn(ctx, "aborting transaction during shutdown; it remains for a later cleanup",
				zap.Stringer("tx", oid), zap.Error(err))
			continue
		}
		s.removeTxIfOrphan(oid)
	}
	return nil
}

func setErrTxOID(err error, oid blobcache.OID) error {
	switch e := err.(type) {
	case blobcache.ErrTxDone:
		e.ID = oid
		return e
	case blobcache.ErrTxReadOnly:
		e.Tx = oid
		return e
	default:
		return err
	}
}
