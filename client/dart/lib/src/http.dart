// Blobcache client over the HTTP+JSON protocol.
//
// This transport uses package:http so it can run both on the VM (via IOClient)
// and in the browser (via BrowserClient). The caller supplies a fully-formed
// base URL; blobcache is assumed to be served same-origin in the browser, so
// no cross-origin (CORS) handling is required here.

import 'dart:convert';
import 'dart:typed_data';

import 'package:http/http.dart' as http;

import 'service.dart';
import 'types.dart';

/// A [Service] implementation that talks HTTP+JSON to a blobcache daemon.
class HttpClient implements Service {
  HttpClient(String endpoint, {http.Client? client})
      : _baseUri = _normalizeEndpoint(endpoint),
        _client = client ?? http.Client();

  /// The normalized `http://host:port` endpoint (no trailing slash).
  final String _baseUri;

  final http.Client _client;

  Future<Map<String, String>> _headers(Handle? secret, CID? salt) async {
    final headers = <String, String>{'Content-Type': 'application/json'};
    if (secret != null) {
      headers['X-Secret'] = secret.secretHex;
    }
    if (salt != null) {
      headers['X-Salt'] = salt.toString();
    }
    return headers;
  }

  Future<http.Response> _send(String method, String path, List<int> body,
      {Handle? secret, CID? salt}) async {
    final h = await _headers(secret, salt);
    final uri = Uri.parse('$_baseUri$path');
    final response = await _client.send(
      http.Request(method, uri)
        ..headers.addAll(h)
        ..bodyBytes = body,
    );
    final full = await http.Response.fromStream(response);
    if (full.statusCode < 200 || full.statusCode >= 300) {
      throw BlobcacheException(
          'request failed with status ${full.statusCode}: ${full.body}');
    }
    return full;
  }

  Future<dynamic> _doJson(String method, String path,
      {Handle? secret, Map<String, dynamic>? body}) async {
    final bytes = utf8.encode(jsonEncode(body ?? {}));
    final response = await _send(method, path, bytes, secret: secret);
    if (response.body.isEmpty) return null;
    return jsonDecode(response.body);
  }

  Future<Uint8List> _doBytes(String method, String path,
      {Handle? secret, CID? salt, required List<int> body}) async {
    final response =
        await _send(method, path, body, secret: secret, salt: salt);
    return response.bodyBytes;
  }

  Future<dynamic> _doGetJson(String path, {Handle? secret}) async {
    final h = await _headers(secret, null);
    final uri = Uri.parse('$_baseUri$path');
    final response = await _client.send(
      http.Request('GET', uri)..headers.addAll(h),
    );
    final full = await http.Response.fromStream(response);
    if (full.statusCode < 200 || full.statusCode >= 300) {
      throw BlobcacheException(
          'request failed with status ${full.statusCode}: ${full.body}');
    }
    return jsonDecode(full.body);
  }

  String _txPath(Handle tx, String method) => '/tx/${tx.oid}.$method';

  @override
  Future<Endpoint> endpoint() async {
    final resp = await _doJson('POST', '/Endpoint', body: {});
    final map = resp as Map<String, dynamic>;
    final ep = map['endpoint'] as Map<String, dynamic>;
    return Endpoint(
      NodeID.fromString(ep['node'] as String),
      ep['ip_port'] as String,
    );
  }

  @override
  Future<HandleInfo> inspectHandle(Handle handle) async {
    final resp = await _doJson('POST', '/InspectHandle',
        body: {'handle': handle.toString()});
    final info = resp['info'] as Map<String, dynamic>;
    return HandleInfo(
      oid: OID.fromString(info['oid'] as String),
      rights: info['rights'] as int,
      createdAt: info['created_at'],
      expiresAt: info['expires_at'],
    );
  }

  @override
  Future<void> dropHandle(Handle handle) async {
    await _doJson('POST', '/Drop',
        secret: handle, body: {'handle': handle.toString()});
  }

  @override
  Future<void> keepAlive(List<Handle> handles) async {
    await _doJson('POST', '/KeepAlive',
        body: {'handles': handles.map((h) => h.toString()).toList()});
  }

  @override
  Future<Handle> shareOut(Handle handle, NodeID to, ActionSet mask) async {
    final resp = await _doJson('POST', '/ShareOut', body: {
      'handle': handle.toString(),
      'peer': to.toString(),
      'mask': mask,
    });
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<Handle> shareIn(NodeID host, Handle handle) async {
    final resp = await _doJson('POST', '/ShareIn', body: {
      'host': host.toString(),
      'handle': handle.toString(),
    });
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<Info> inspect(Handle handle) async {
    final resp = await _doJson('POST', '/Inspect',
        body: {'handle': handle.toString()});
    final info = resp['info'] as Map<String, dynamic>;
    return Info(
      handle: HandleInfo(
        oid: OID.fromString((info['handle'] as Map)['oid'] as String),
        rights: (info['handle'] as Map)['rights'] as int,
        createdAt: (info['handle'] as Map)['created_at'],
        expiresAt: (info['handle'] as Map)['expires_at'],
      ),
    );
  }

  @override
  Future<Handle> openFiat(OID target, ActionSet mask) async {
    final resp = await _doJson('POST', '/OpenFiat',
        body: {'target': target.toString(), 'mask': mask});
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<Handle> openFrom(Handle base, LinkToken token, ActionSet mask) async {
    final resp = await _doJson('POST', '/OpenFrom', body: {
      'base': base.toString(),
      'token': _linkTokenJson(token),
      'mask': mask,
    });
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<Handle> createVolume(Endpoint? host, VolumeSpec spec) async {
    final resp = await _doJson('POST', '/volume/', body: {
      'host': host?.toJson(),
      'spec': spec.toJson(),
    });
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<Handle> cloneVolume(Handle volume) async {
    final resp = await _doJson('POST', '/volume/Clone',
        body: {'volume': volume.toString()});
    return Handle.fromString(resp['clone'] as String);
  }

  @override
  Future<VolumeInfo> inspectVolume(Handle volume) async {
    final info =
        await _doGetJson('/volume/${volume.oid}.Inspect', secret: volume)
            as Map<String, dynamic>;
    return VolumeInfo(
      id: OID.fromString(info['id'] as String),
      hashAlgo: ((info['backend'] as Map)['local'] as Map)['hash_algo'] as String,
    );
  }

  @override
  Future<Handle> beginTx(Handle volume, TxParams params) async {
    final resp = await _doJson('POST', '/tx/',
        secret: volume,
        body: {'volume': volume.toString(), 'params': params.toJson()});
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<TxInfo> inspectTx(Handle tx) async {
    final resp = await _doJson('POST', '/tx/',
        secret: tx, body: {'tx': tx.toString()});
    final info = resp['info'] as Map<String, dynamic>;
    return TxInfo(
      id: OID.fromString(info['ID'] as String),
      volume: OID.fromString(info['Volume'] as String),
      maxSize: info['MaxSize'] as int,
      hashAlgo: info['HashAlgo'] as String,
    );
  }

  @override
  Future<void> commit(Handle tx) async {
    await _doJson('POST', _txPath(tx, 'Commit'), secret: tx, body: {});
  }

  @override
  Future<void> abort(Handle tx) async {
    await _doJson('POST', _txPath(tx, 'Abort'), secret: tx, body: {});
  }

  @override
  Future<void> save(Handle tx, List<int> root) async {
    await _doJson('POST', _txPath(tx, 'Save'),
        secret: tx, body: {'root': base64.encode(root)});
  }

  @override
  Future<Uint8List> load(Handle tx) async {
    final resp = await _doJson('POST', _txPath(tx, 'Load'),
        secret: tx, body: {});
    return base64.decode(resp['root'] as String);
  }

  @override
  Future<CID> post(Handle tx, List<int> data, PostOpts opts) async {
    final bytes = await _doBytes('POST', _txPath(tx, 'Post'),
        secret: tx, salt: opts.salt, body: data);
    return CID.fromBytes(bytes);
  }

  @override
  Future<Uint8List> get(Handle tx, CID cid, GetOpts opts) async {
    final body = jsonEncode({
      'cid': cid.toString(),
      if (opts.salt != null) 'salt': opts.salt!.toString(),
    });
    return _doBytes('POST', _txPath(tx, 'Get'),
        secret: tx, body: utf8.encode(body));
  }

  @override
  Future<List<bool>> exists(Handle tx, List<CID> cids) async {
    final resp = await _doJson('POST', _txPath(tx, 'Exists'),
        secret: tx, body: {'cids': cids.map((c) => c.toString()).toList()});
    return (resp['exists'] as List).cast<bool>();
  }

  @override
  Future<void> delete(Handle tx, List<CID> cids) async {
    await _doJson('POST', _txPath(tx, 'Delete'),
        secret: tx, body: {'cids': cids.map((c) => c.toString()).toList()});
  }

  @override
  Future<List<bool>> copy(
      Handle tx, List<Handle> srcTxs, List<CID> cids) async {
    final resp = await _doJson('POST', _txPath(tx, 'AddFrom'),
        secret: tx,
        body: {
          'cids': cids.map((c) => c.toString()).toList(),
          'srcs': srcTxs.map((s) => s.toString()).toList(),
        });
    return (resp['added'] as List).cast<bool>();
  }

  @override
  Future<void> visit(Handle tx, List<CID> cids) async {
    await _doJson('POST', _txPath(tx, 'Visit'),
        secret: tx, body: {'cids': cids.map((c) => c.toString()).toList()});
  }

  @override
  Future<List<bool>> isVisited(Handle tx, List<CID> cids) async {
    final resp = await _doJson('POST', _txPath(tx, 'IsVisited'),
        secret: tx, body: {'cids': cids.map((c) => c.toString()).toList()});
    return (resp['visited'] as List).cast<bool>();
  }

  @override
  Future<LinkToken> link(Handle tx, Handle target, ActionSet mask) async {
    final resp = await _doJson('POST', _txPath(tx, 'Link'),
        secret: tx, body: {'target': target.toString(), 'mask': mask});
    return _linkTokenFromJson(resp['token'] as Map<String, dynamic>);
  }

  @override
  Future<void> unlink(Handle tx, List<LinkToken> targets) async {
    await _doJson('POST', _txPath(tx, 'Unlink'),
        secret: tx,
        body: {'targets': targets.map(_linkTokenJson).toList()});
  }

  @override
  Future<void> visitLinks(Handle tx, List<LinkToken> targets) async {
    await _doJson('POST', _txPath(tx, 'VisitLinks'),
        secret: tx,
        body: {'targets': targets.map(_linkTokenJson).toList()});
  }

  @override
  Future<Handle> createQueue(Endpoint? host, QueueSpec spec) async {
    final resp = await _doJson('POST', '/queue/', body: {
      'host': host?.toJson(),
      'spec': spec.toJson(),
    });
    return Handle.fromString(resp['handle'] as String);
  }

  @override
  Future<QueueInfo> inspectQueue(Handle queue) async {
    final info =
        await _doGetJson('/queue/${queue.oid}.Inspect', secret: queue)
            as Map<String, dynamic>;
    final config = info['config'] as Map<String, dynamic>;
    return QueueInfo(
      id: OID.fromString(info['id'] as String),
      config: QueueConfig(
        maxDepth: config['max_depth'] as int,
        maxBytesPerMessage: config['max_bytes_per_message'] as int,
        maxHandlesPerMessage: config['max_handles_per_message'] as int,
      ),
    );
  }

  @override
  Future<List<Message>> dequeue(
      Handle queue, int max, DequeueOpts opts) async {
    final resp = await _doJson('POST', '/queue/${queue.oid}.Dequeue',
        secret: queue, body: {'opts': opts.toJson(), 'max': max});
    final messages = resp['messages'] as List;
    return messages.map((m) => _messageFromJson(m as Map<String, dynamic>))
        .toList();
  }

  @override
  Future<InsertResp> enqueue(Handle queue, List<Message> messages) async {
    final resp = await _doJson('POST', '/queue/${queue.oid}.Enqueue',
        secret: queue,
        body: {'messages': messages.map((m) => m.toJson()).toList()});
    return InsertResp(success: resp['success'] as int);
  }

  @override
  Future<void> subToVolume(
      Handle queue, Handle volume, VolSubSpec spec) async {
    await _doJson('POST', '/queue/${queue.oid}.SubToVolume',
        secret: queue,
        body: {'volume': volume.toString(), 'spec': spec.toJson()});
  }
}

String _normalizeEndpoint(String endpoint) {
  if (endpoint.startsWith('unix://')) {
    throw BlobcacheException('unsupported endpoint for HTTP client: $endpoint');
  }
  var rest = endpoint;
  if (rest.startsWith('http://')) rest = rest.substring('http://'.length);
  if (rest.startsWith('tcp://')) rest = rest.substring('tcp://'.length);
  if (rest.endsWith('/')) rest = rest.substring(0, rest.length - 1);
  return 'http://$rest';
}

Map<String, dynamic> _linkTokenJson(LinkToken token) => {
      'target': token.target.toString(),
      'rights': token.rights,
      'secret': token.secretHex,
    };

LinkToken _linkTokenFromJson(Map<String, dynamic> json) => LinkToken(
      OID.fromString(json['target'] as String),
      json['rights'] as int,
      Uint8List.fromList(_hexDecode(json['secret'] as String)),
    );

Message _messageFromJson(Map<String, dynamic> json) => Message(
      handles: (json['handles'] as List)
          .map((h) => Handle.fromString(h as String))
          .toList(),
      bytes: base64.decode(json['bytes'] as String),
    );

List<int> _hexDecode(String s) {
  final out = <int>[];
  for (var i = 0; i < s.length; i += 2) {
    out.add(int.parse(s.substring(i, i + 2), radix: 16));
  }
  return out;
}