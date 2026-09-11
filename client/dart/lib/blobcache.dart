// Client factory and public API re-exports for the blobcache package.

import 'dart:typed_data';

import 'src/http.dart';
import 'src/service.dart';
import 'src/types.dart';

import 'src/env_stub.dart' if (dart.library.io) 'src/env_io.dart';
import 'src/unix_client_stub.dart'
    if (dart.library.io) 'src/unix_client_io.dart';

export 'src/service.dart';
export 'src/types.dart';
export 'src/http.dart' show HttpClient;

/// The name of the environment variable used as the API endpoint.
const String envBlobcacheApi = 'BLOBCACHE_API';

/// The name of the environment variable used as the namespace root.
const String envBlobcacheNsRoot = 'BLOBCACHE_NS_ROOT';

/// The default endpoint assumed when [envBlobcacheApi] is unset.
const String defaultEndpoint = 'unix:///run/blobcache/blobcache.sock';

/// A client that dispatches to the appropriate transport based on the
/// endpoint scheme.
class Client implements Service {
  Client._(this._inner);

  final Service _inner;

  factory Client(String endpoint) {
    if (endpoint.startsWith('unix://')) {
      return Client._(buildUnixClient(endpoint.substring('unix://'.length)));
    }
    return Client._(HttpClient(endpoint));
  }

  factory Client.fromEnv() {
    final endpoint = readEnvironment(envBlobcacheApi) ?? defaultEndpoint;
    return Client(endpoint);
  }

  @override
  Future<Endpoint> endpoint() => _inner.endpoint();

  @override
  Future<HandleInfo> inspectHandle(Handle handle) =>
      _inner.inspectHandle(handle);

  @override
  Future<void> dropHandle(Handle handle) => _inner.dropHandle(handle);

  @override
  Future<void> keepAlive(List<Handle> handles) => _inner.keepAlive(handles);

  @override
  Future<Handle> shareOut(Handle handle, NodeID to, ActionSet mask) =>
      _inner.shareOut(handle, to, mask);

  @override
  Future<Handle> shareIn(NodeID host, Handle handle) =>
      _inner.shareIn(host, handle);

  @override
  Future<Info> inspect(Handle handle) => _inner.inspect(handle);

  @override
  Future<Handle> openFiat(OID target, ActionSet mask) =>
      _inner.openFiat(target, mask);

  @override
  Future<Handle> openFrom(Handle base, LinkToken token, ActionSet mask) =>
      _inner.openFrom(base, token, mask);

  @override
  Future<Handle> createVolume(Endpoint? host, VolumeSpec spec) =>
      _inner.createVolume(host, spec);

  @override
  Future<Handle> cloneVolume(Handle volume) => _inner.cloneVolume(volume);

  @override
  Future<VolumeInfo> inspectVolume(Handle volume) =>
      _inner.inspectVolume(volume);

  @override
  Future<Handle> beginTx(Handle volume, TxParams params) =>
      _inner.beginTx(volume, params);

  @override
  Future<TxInfo> inspectTx(Handle tx) => _inner.inspectTx(tx);

  @override
  Future<void> commit(Handle tx) => _inner.commit(tx);

  @override
  Future<void> abort(Handle tx) => _inner.abort(tx);

  @override
  Future<void> save(Handle tx, List<int> root) => _inner.save(tx, root);

  @override
  Future<Uint8List> load(Handle tx) => _inner.load(tx);

  @override
  Future<CID> post(Handle tx, List<int> data, PostOpts opts) =>
      _inner.post(tx, data, opts);

  @override
  Future<Uint8List> get(Handle tx, CID cid, GetOpts opts) =>
      _inner.get(tx, cid, opts);

  @override
  Future<List<bool>> exists(Handle tx, List<CID> cids) =>
      _inner.exists(tx, cids);

  @override
  Future<void> delete(Handle tx, List<CID> cids) => _inner.delete(tx, cids);

  @override
  Future<List<bool>> copy(
          Handle tx, List<Handle> srcTxs, List<CID> cids) =>
      _inner.copy(tx, srcTxs, cids);

  @override
  Future<void> visit(Handle tx, List<CID> cids) => _inner.visit(tx, cids);

  @override
  Future<List<bool>> isVisited(Handle tx, List<CID> cids) =>
      _inner.isVisited(tx, cids);

  @override
  Future<LinkToken> link(Handle tx, Handle target, ActionSet mask) =>
      _inner.link(tx, target, mask);

  @override
  Future<void> unlink(Handle tx, List<LinkToken> targets) =>
      _inner.unlink(tx, targets);

  @override
  Future<void> visitLinks(Handle tx, List<LinkToken> targets) =>
      _inner.visitLinks(tx, targets);

  @override
  Future<Handle> createQueue(Endpoint? host, QueueSpec spec) =>
      _inner.createQueue(host, spec);

  @override
  Future<QueueInfo> inspectQueue(Handle queue) => _inner.inspectQueue(queue);

  @override
  Future<List<Message>> dequeue(Handle queue, int max, DequeueOpts opts) =>
      _inner.dequeue(queue, max, opts);

  @override
  Future<InsertResp> enqueue(Handle queue, List<Message> messages) =>
      _inner.enqueue(queue, messages);

  @override
  Future<void> subToVolume(Handle queue, Handle volume, VolSubSpec spec) =>
      _inner.subToVolume(queue, volume, spec);
}

/// Creates a client backed by the server at [endpoint].
Client newClient(String endpoint) => Client(endpoint);

/// Creates a client from the [envBlobcacheApi] environment variable.
Client newClientFromEnv() => Client.fromEnv();