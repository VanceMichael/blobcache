// Blobcache client over the BCP protocol transported on a unix socket.

import 'dart:convert';
import 'dart:typed_data';

import 'bcp.dart';
import 'service.dart';
import 'types.dart';
import 'unix_socket.dart';

/// A [Service] implementation that talks BCP over a unix domain socket.
class UNIXClient implements Service {
  UNIXClient(this.socketPath);

  final String socketPath;

  Future<List<int>> _ask(MessageCode code, List<int> body) async {
    final socket = UnixDomainSocket.connect(socketPath);
    try {
      socket.write(writeMessage(code, body));

      // Read the 8-byte header.
      final header = socket.read(headerLen);
      if (header.length < headerLen) {
        throw BlobcacheException('truncated response header');
      }
      final respCode = (header[0] << 8) | header[1];
      final bodyLen = header[4] |
          (header[5] << 8) |
          (header[6] << 16) |
          (header[7] << 24);
      final respBody = socket.read(bodyLen);
      checkOk(respCode, respBody);
      return respBody;
    } finally {
      socket.close();
    }
  }

  @override
  Future<Endpoint> endpoint() async {
    final body = await _ask(mtEndpoint, []);
    return _endpointFromBcp(body);
  }

  @override
  Future<HandleInfo> inspectHandle(Handle handle) async {
    final body = await _ask(mtHandleInspect, _marshalHandle(handle));
    if (body.length < 40) {
      throw BlobcacheException('inspect handle response too short');
    }
    final oid = OID.fromBytes(body.sublist(0, 16));
    final rights = _leBytesToInt(body.sublist(16, 24));
    final createdAt = _hexEncode(body.sublist(24, 32));
    final expiresAt = _hexEncode(body.sublist(32, 40));
    return HandleInfo(
      oid: oid,
      rights: rights,
      createdAt: createdAt,
      expiresAt: expiresAt,
    );
  }

  @override
  Future<void> dropHandle(Handle handle) async {
    await _ask(mtHandleDrop, _marshalHandle(handle));
  }

  @override
  Future<void> keepAlive(List<Handle> handles) async {
    final req = BytesBuilder();
    for (final h in handles) {
      req.add(_marshalHandle(h));
    }
    await _ask(mtHandleKeepAlive, req.takeBytes());
  }

  @override
  Future<Handle> shareOut(Handle handle, NodeID to, ActionSet mask) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(handle));
    req.add(to.asBytes());
    req.add(_beBytes(mask));
    final body = await _ask(mtHandleShareOut, req.takeBytes());
    return Handle.unmarshal(body);
  }

  @override
  Future<Handle> shareIn(NodeID host, Handle handle) async {
    final req = BytesBuilder();
    req.add(host.asBytes());
    req.add(_marshalHandle(handle));
    final body = await _ask(mtHandleShareIn, req.takeBytes());
    return Handle.unmarshal(body);
  }

  @override
  Future<Info> inspect(Handle handle) async {
    final body = await _ask(mtInspect, _marshalHandle(handle));
    return _infoFromJson(jsonDecode(utf8.decode(body)));
  }

  @override
  Future<Handle> openFiat(OID target, ActionSet mask) async {
    final req = BytesBuilder();
    req.add(target.asBytes());
    req.add(_beBytes(mask));
    final body = await _ask(mtOpenFiat, req.takeBytes());
    if (body.length < nodeIdSize + handleSize) {
      throw BlobcacheException('open fiat response too short');
    }
    return Handle.unmarshal(body.sublist(nodeIdSize, nodeIdSize + handleSize));
  }

  @override
  Future<Handle> openFrom(Handle base, LinkToken token, ActionSet mask) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(base));
    req.add(token.marshal());
    req.add(_beBytes(mask));
    final body = await _ask(mtOpenFrom, req.takeBytes());
    if (body.length < nodeIdSize + handleSize) {
      throw BlobcacheException('open from response too short');
    }
    return Handle.unmarshal(body.sublist(nodeIdSize, nodeIdSize + handleSize));
  }

  @override
  Future<Handle> createVolume(Endpoint? host, VolumeSpec spec) async {
    final req = BytesBuilder();
    _writeHost(req, host);
    final specData = utf8.encode(jsonEncode(spec.toJson()));
    req.add(_leU16(specData.length));
    req.add(specData);
    final body = await _ask(mtCreateVolume, req.takeBytes());
    if (body.length < handleSize) {
      throw BlobcacheException('create volume response too short');
    }
    return Handle.unmarshal(body.sublist(0, handleSize));
  }

  @override
  Future<Handle> cloneVolume(Handle volume) async {
    throw BlobcacheException(
        'clone volume is not supported by the BCP wire protocol');
  }

  @override
  Future<VolumeInfo> inspectVolume(Handle volume) async {
    final body = await _ask(mtVolumeInspect, _marshalHandle(volume));
    return _volumeInfoFromJson(jsonDecode(utf8.decode(body)));
  }

  @override
  Future<Handle> beginTx(Handle volume, TxParams params) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(volume));
    req.add(params.marshal());
    final body = await _ask(mtVolumeBeginTx, req.takeBytes());
    if (body.length < handleSize) {
      throw BlobcacheException('begin tx response too short');
    }
    return Handle.unmarshal(body.sublist(0, handleSize));
  }

  @override
  Future<TxInfo> inspectTx(Handle tx) async {
    final body = await _ask(mtTxInspect, _marshalHandle(tx));
    return _txInfoFromJson(jsonDecode(utf8.decode(body)));
  }

  @override
  Future<void> commit(Handle tx) async {
    await _ask(mtTxCommit, _marshalHandle(tx));
  }

  @override
  Future<void> abort(Handle tx) async {
    await _ask(mtTxAbort, _marshalHandle(tx));
  }

  @override
  Future<void> save(Handle tx, List<int> root) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    req.add(root);
    await _ask(mtTxSave, req.takeBytes());
  }

  @override
  Future<Uint8List> load(Handle tx) async {
    final body = await _ask(mtTxLoad, _marshalHandle(tx));
    return Uint8List.fromList(body);
  }

  @override
  Future<CID> post(Handle tx, List<int> data, PostOpts opts) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    final code = opts.salt != null ? mtTxPostSalt : mtTxPost;
    if (opts.salt != null) {
      req.add(opts.salt!.asBytes());
    }
    req.add(data);
    final body = await _ask(code, req.takeBytes());
    return CID.fromBytes(body);
  }

  @override
  Future<Uint8List> get(Handle tx, CID cid, GetOpts opts) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    final code = opts.salt != null ? mtTxGetSalt : mtTxGet;
    if (opts.salt != null) {
      req.add(opts.salt!.asBytes());
    }
    req.add(cid.asBytes());
    final body = await _ask(code, req.takeBytes());
    return Uint8List.fromList(body);
  }

  @override
  Future<List<bool>> exists(Handle tx, List<CID> cids) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    for (final cid in cids) {
      req.add(cid.asBytes());
    }
    final body = await _ask(mtTxExists, req.takeBytes());
    return decodeBools(body, cids.length);
  }

  @override
  Future<void> delete(Handle tx, List<CID> cids) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, cids.length);
    for (final cid in cids) {
      req.add(cid.asBytes());
    }
    await _ask(mtTxDelete, req.takeBytes());
  }

  @override
  Future<List<bool>> copy(
      Handle tx, List<Handle> srcTxs, List<CID> cids) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, cids.length);
    for (final cid in cids) {
      req.add(cid.asBytes());
    }
    writeUvarint(req, srcTxs.length);
    for (final src in srcTxs) {
      req.add(_marshalHandle(src));
    }
    final body = await _ask(mtTxCopy, req.takeBytes());
    return decodeBools(body, cids.length);
  }

  @override
  Future<void> visit(Handle tx, List<CID> cids) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, cids.length);
    for (final cid in cids) {
      req.add(cid.asBytes());
    }
    await _ask(mtTxVisit, req.takeBytes());
  }

  @override
  Future<List<bool>> isVisited(Handle tx, List<CID> cids) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, cids.length);
    for (final cid in cids) {
      req.add(cid.asBytes());
    }
    final body = await _ask(mtTxIsVisited, req.takeBytes());
    return decodeBools(body, cids.length);
  }

  @override
  Future<LinkToken> link(Handle tx, Handle target, ActionSet mask) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    req.add(_marshalHandle(target));
    req.add(_beBytes(mask));
    final body = await _ask(mtTxLink, req.takeBytes());
    return LinkToken.unmarshal(body);
  }

  @override
  Future<void> unlink(Handle tx, List<LinkToken> targets) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, targets.length);
    for (final token in targets) {
      req.add(token.marshal());
    }
    await _ask(mtTxUnlink, req.takeBytes());
  }

  @override
  Future<void> visitLinks(Handle tx, List<LinkToken> targets) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(tx));
    writeUvarint(req, targets.length);
    for (final token in targets) {
      req.add(token.marshal());
    }
    await _ask(mtTxVisitLinks, req.takeBytes());
  }

  @override
  Future<Handle> createQueue(Endpoint? host, QueueSpec spec) async {
    final req = BytesBuilder();
    _writeHost(req, host);
    final specData = utf8.encode(jsonEncode(spec.toJson()));
    req.add(_leU16(specData.length));
    req.add(specData);
    final body = await _ask(mtQueueCreate, req.takeBytes());
    return Handle.unmarshal(body);
  }

  @override
  Future<QueueInfo> inspectQueue(Handle queue) async {
    final body = await _ask(mtQueueInspect, _marshalHandle(queue));
    return _queueInfoFromJson(jsonDecode(utf8.decode(body)));
  }

  @override
  Future<List<Message>> dequeue(
      Handle queue, int max, DequeueOpts opts) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(queue));
    req.add(_leU32(max));
    req.add(utf8.encode(jsonEncode(opts.toJson())));
    final body = await _ask(mtQueueDequeue, req.takeBytes());
    if (body.length < 4) {
      throw BlobcacheException('dequeue response too short');
    }
    var idx = 4;
    final num = _leBytesToInt(body.sublist(0, 4));
    final out = <Message>[];
    for (var i = 0; i < num; i++) {
      final (msgData, off) = readLp(body, idx);
      out.add(Message.unmarshal(msgData));
      idx = off;
    }
    return out;
  }

  @override
  Future<InsertResp> enqueue(Handle queue, List<Message> messages) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(queue));
    req.add(_leU32(messages.length));
    for (final msg in messages) {
      appendLp(req, msg.marshal());
    }
    final body = await _ask(mtQueueEnqueue, req.takeBytes());
    if (body.length < 4) {
      throw BlobcacheException('enqueue response too short');
    }
    return InsertResp(success: _leBytesToInt(body.sublist(0, 4)));
  }

  @override
  Future<void> subToVolume(
      Handle queue, Handle volume, VolSubSpec spec) async {
    final req = BytesBuilder();
    req.add(_marshalHandle(queue));
    req.add(_marshalHandle(volume));
    appendLp(req, utf8.encode(jsonEncode(spec.toJson())));
    await _ask(mtQueueSubToVolume, req.takeBytes());
  }
}

// --- internal helpers (shared with tests via library privacy) ---

List<int> _marshalHandle(Handle h) => h.marshal();

List<int> _beBytes(int v) {
  // 8-byte big endian mask (ActionSet).
  return [
    (v >> 56) & 0xff,
    (v >> 48) & 0xff,
    (v >> 40) & 0xff,
    (v >> 32) & 0xff,
    (v >> 24) & 0xff,
    (v >> 16) & 0xff,
    (v >> 8) & 0xff,
    v & 0xff,
  ];
}

List<int> _leU16(int v) => [v & 0xff, (v >> 8) & 0xff];

List<int> _leU32(int v) => [v & 0xff, (v >> 8) & 0xff, (v >> 16) & 0xff, (v >> 24) & 0xff];

int _leBytesToInt(List<int> bytes) {
  var value = 0;
  for (var i = bytes.length - 1; i >= 0; i--) {
    value = (value << 8) | bytes[i];
  }
  return value;
}

String _hexEncode(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

void _writeHost(BytesBuilder req, Endpoint? host) {
  if (host == null) {
    final hostData = List.filled(nodeIdSize, 0);
    req.add(_leU16(hostData.length));
    req.add(hostData);
    return;
  }
  // Node id bytes followed by ip:port.
  final ipPort = host.ipPort;
  List<int> addrBytes;
  if (ipPort.contains(':')) {
    final parts = ipPort.split(':');
    final ip = parts[0];
    final port = int.parse(parts[1]);
    addrBytes = _parseIp(ip);
    addrBytes.add(port & 0xff);
    addrBytes.add((port >> 8) & 0xff);
  } else {
    addrBytes = [];
  }
  final hostData = [...host.node.asBytes(), ...addrBytes];
  req.add(_leU16(hostData.length));
  req.add(hostData);
}

List<int> _parseIp(String ip) {
  if (ip.contains('.')) {
    return ip.split('.').map(int.parse).toList();
  }
  throw BlobcacheException('ipv6 endpoints are not supported');
}

Endpoint _endpointFromBcp(List<int> body) {
  if (body.length < nodeIdSize) {
    throw BlobcacheException('endpoint response too short');
  }
  final node = NodeID.fromBytes(body.sublist(0, nodeIdSize));
  final ap = body.sublist(nodeIdSize);
  final ipPort = ap.isEmpty ? '' : '${ap[0]}.${ap[1]}.${ap[2]}.${ap[3]}:${(ap[4]) | (ap[5] << 8)}';
  return Endpoint(node, ipPort);
}

Info _infoFromJson(dynamic json) {
  final map = json as Map<String, dynamic>;
  final handle = map['handle'] as Map<String, dynamic>;
  final handleInfo = HandleInfo(
    oid: OID.fromString(handle['oid'] as String),
    rights: handle['rights'] as int,
    createdAt: handle['created_at'],
    expiresAt: handle['expires_at'],
  );
  final volumeJson = map['volume'];
  final txJson = map['tx'];
  return Info(
    handle: handleInfo,
    volume: volumeJson == null ? null : _volumeInfoFromJson(volumeJson),
    tx: txJson == null ? null : _txInfoFromJson(txJson),
  );
}

VolumeInfo _volumeInfoFromJson(dynamic json) {
  final map = json as Map<String, dynamic>;
  return VolumeInfo(
    id: OID.fromString(map['id'] as String),
    hashAlgo: ((map['backend'] as Map)['local'] as Map)['hash_algo'] as String,
  );
}

TxInfo _txInfoFromJson(dynamic json) {
  final map = json as Map<String, dynamic>;
  return TxInfo(
    id: OID.fromString(map['ID'] as String),
    volume: OID.fromString(map['Volume'] as String),
    maxSize: map['MaxSize'] as int,
    hashAlgo: map['HashAlgo'] as String,
  );
}

QueueInfo _queueInfoFromJson(dynamic json) {
  final map = json as Map<String, dynamic>;
  final config = map['config'] as Map<String, dynamic>;
  return QueueInfo(
    id: OID.fromString(map['id'] as String),
    config: QueueConfig(
      maxDepth: config['max_depth'] as int,
      maxBytesPerMessage: config['max_bytes_per_message'] as int,
      maxHandlesPerMessage: config['max_handles_per_message'] as int,
    ),
  );
}