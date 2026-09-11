// The interface implemented by all blobcache transports.

import 'dart:typed_data';

import 'types.dart';

/// The unified client interface for the Blobcache API.
abstract class Service {
  Future<Endpoint> endpoint();

  Future<HandleInfo> inspectHandle(Handle handle);
  Future<void> dropHandle(Handle handle);
  Future<void> keepAlive(List<Handle> handles);

  Future<Handle> shareOut(Handle handle, NodeID to, ActionSet mask);
  Future<Handle> shareIn(NodeID host, Handle handle);
  Future<Info> inspect(Handle handle);

  Future<Handle> openFiat(OID target, ActionSet mask);
  Future<Handle> openFrom(Handle base, LinkToken token, ActionSet mask);
  Future<Handle> createVolume(Endpoint? host, VolumeSpec spec);
  Future<Handle> cloneVolume(Handle volume);
  Future<VolumeInfo> inspectVolume(Handle volume);

  Future<Handle> beginTx(Handle volume, TxParams params);
  Future<TxInfo> inspectTx(Handle tx);
  Future<void> commit(Handle tx);
  Future<void> abort(Handle tx);
  Future<void> save(Handle tx, List<int> root);
  Future<Uint8List> load(Handle tx);
  Future<CID> post(Handle tx, List<int> data, PostOpts opts);
  Future<Uint8List> get(Handle tx, CID cid, GetOpts opts);
  Future<List<bool>> exists(Handle tx, List<CID> cids);
  Future<void> delete(Handle tx, List<CID> cids);
  Future<List<bool>> copy(Handle tx, List<Handle> srcTxs, List<CID> cids);
  Future<void> visit(Handle tx, List<CID> cids);
  Future<List<bool>> isVisited(Handle tx, List<CID> cids);
  Future<LinkToken> link(Handle tx, Handle target, ActionSet mask);
  Future<void> unlink(Handle tx, List<LinkToken> targets);
  Future<void> visitLinks(Handle tx, List<LinkToken> targets);

  Future<Handle> createQueue(Endpoint? host, QueueSpec spec);
  Future<QueueInfo> inspectQueue(Handle queue);
  Future<List<Message>> dequeue(Handle queue, int max, DequeueOpts opts);
  Future<InsertResp> enqueue(Handle queue, List<Message> messages);
  Future<void> subToVolume(Handle queue, Handle volume, VolSubSpec spec);
}

/// Information returned by [Service.inspect].
class Info {
  const Info({required this.handle, this.volume, this.tx, this.queue});

  final HandleInfo handle;
  final VolumeInfo? volume;
  final TxInfo? tx;
  final QueueInfo? queue;
}

/// Information returned when inspecting a queue.
class QueueInfo {
  const QueueInfo({required this.id, required this.config});

  final OID id;
  final QueueConfig config;
}

/// Configuration for a queue.
class QueueConfig {
  const QueueConfig({
    required this.maxDepth,
    required this.maxBytesPerMessage,
    required this.maxHandlesPerMessage,
  });

  final int maxDepth;
  final int maxBytesPerMessage;
  final int maxHandlesPerMessage;
}

/// Subscription spec for [Service.subToVolume].
class VolSubSpec {
  const VolSubSpec({
    this.beginTx,
    this.sendCell = false,
    this.sendBlobs = false,
  });

  final TxParams? beginTx;
  final bool sendCell;
  final bool sendBlobs;

  Map<String, dynamic> toJson() => {
        if (beginTx != null) 'begin_tx': beginTx!.toJson(),
        'send_cell': sendCell,
        'send_blobs': sendBlobs,
      };
}