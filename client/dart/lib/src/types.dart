// Core value types and specification/info structs for the Blobcache API.

import 'dart:convert';
import 'dart:typed_data';

const int oidSize = 16;
const int cidSize = 32;
const int handleSize = oidSize + 16;
const int linkTokenSize = 16 + 8 + 24;
const int nodeIdSize = 32;

/// The set of rights granted to a handle, encoded as a bitmask.
typedef ActionSet = int;

/// The lexicographically-sortable base64 alphabet used to encode CIDs.
const String _cidAlphabet =
    '-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz';

/// An error produced by the blobcache client.
class BlobcacheException implements Exception {
  const BlobcacheException(this.message);

  final String message;

  @override
  String toString() => 'BlobcacheException: $message';
}

/// A 16-byte object identifier.
class OID {
  const OID(this._bytes);

  final Uint8List _bytes;

  static OID fromBytes(List<int> data) {
    if (data.length != oidSize) {
      throw BlobcacheException('invalid oid: wrong length ${data.length}');
    }
    return OID(Uint8List.fromList(data));
  }

  static OID fromString(String s) {
    final compact = s.replaceAll('_', '');
    if (compact.length != oidSize * 2) {
      throw BlobcacheException('invalid oid: $s');
    }
    final bytes = Uint8List.fromList(_hexDecode(compact));
    return fromBytes(bytes);
  }

  Uint8List asBytes() => Uint8List.fromList(_bytes);

  @override
  String toString() => _hexEncode(_bytes).toUpperCase();
}

/// A 32-byte content identifier (a cryptographic hash).
class CID {
  const CID(this._bytes);

  final Uint8List _bytes;

  static CID fromBytes(List<int> data) {
    if (data.length != cidSize) {
      throw BlobcacheException('invalid cid: wrong length ${data.length}');
    }
    return CID(Uint8List.fromList(data));
  }

  static CID fromString(String s) {
    final bytes = _cidDecode(s);
    if (bytes.length != cidSize) {
      throw BlobcacheException('invalid cid: $s');
    }
    return CID(Uint8List.fromList(bytes));
  }

  Uint8List asBytes() => Uint8List.fromList(_bytes);

  @override
  String toString() => _cidEncode(_bytes);
}

/// A 32-byte node identifier.
class NodeID {
  const NodeID(this._bytes);

  final Uint8List _bytes;

  static NodeID fromBytes(List<int> data) {
    if (data.length != nodeIdSize) {
      throw BlobcacheException('invalid peer id: wrong length ${data.length}');
    }
    return NodeID(Uint8List.fromList(data));
  }

  static NodeID fromString(String s) {
    final decoded = base64Url.decode(s);
    return fromBytes(decoded);
  }

  Uint8List asBytes() => Uint8List.fromList(_bytes);

  @override
  String toString() => base64Url.encode(_bytes).replaceAll('=', '');
}

/// A handle references an object (volume, tx, queue, ...) and carries a
/// capability secret used to authorize access.
class Handle {
  const Handle(this.oid, this._secret);

  final OID oid;
  final Uint8List _secret;

  static Handle fromString(String s) {
    final dot = s.indexOf('.');
    if (dot < 0) {
      throw BlobcacheException('invalid handle: $s');
    }
    final oid = OID.fromString(s.substring(0, dot));
    final secret = _hexDecode(s.substring(dot + 1));
    if (secret.length != 16) {
      throw BlobcacheException('invalid handle: $s');
    }
    return Handle(oid, Uint8List.fromList(secret));
  }

  static Handle unmarshal(List<int> data) {
    if (data.length < handleSize) {
      throw BlobcacheException('invalid handle: wrong length ${data.length}');
    }
    final oid = OID.fromBytes(data.sublist(0, oidSize));
    final secret = Uint8List.fromList(data.sublist(oidSize, handleSize));
    return Handle(oid, secret);
  }

  String get secretHex => _hexEncode(_secret);

  /// The raw 16 secret bytes.
  Uint8List secretBytes() => _secret;

  /// Marshals this handle to its 32-byte binary form.
  List<int> marshal() => [...oid.asBytes(), ..._secret];

  @override
  String toString() => '$oid.$secretHex';
}

/// A link token produced by [Service.link].
class LinkToken {
  const LinkToken(this.target, this.rights, this._secret);

  final OID target;
  final ActionSet rights;
  final Uint8List _secret;

  static LinkToken unmarshal(List<int> data) {
    if (data.length < linkTokenSize) {
      throw BlobcacheException(
          'invalid link token: wrong length ${data.length}');
    }
    final target = OID.fromBytes(data.sublist(0, 16));
    final rightsBytes = data.sublist(16, 24);
    final rights = _leBytesToInt(rightsBytes);
    final secret = Uint8List.fromList(data.sublist(24, 48));
    return LinkToken(target, rights, secret);
  }

  String get secretHex => _hexEncode(_secret);

  /// Marshals this token to its 48-byte binary form (target + rights + secret).
  List<int> marshal() => [
        ...target.asBytes(),
        ..._leBytes(rights, 8),
        ..._secret,
      ];

  @override
  String toString() => '${target.toString()}.$rights.$secretHex';
}

/// A node endpoint: the node id plus an ip:port address string.
class Endpoint {
  const Endpoint(this.node, this.ipPort);

  final NodeID node;
  final String ipPort;

  Map<String, dynamic> toJson() => {'node': node.toString(), 'ip_port': ipPort};
}

/// Authorization parameters for a transaction.
class TxParams {
  const TxParams({
    this.modify = false,
    this.gcBlobs = false,
    this.gcLinks = false,
  });

  final bool modify;
  final bool gcBlobs;
  final bool gcLinks;

  Map<String, dynamic> toJson() =>
      {'modify': modify, 'gc_blobs': gcBlobs, 'gc_links': gcLinks};

  /// Marshals this params struct to its 4-byte flag form.
  List<int> marshal() {
    var flags = 0;
    if (modify) flags |= 1 << 0;
    if (gcBlobs) flags |= 1 << 1;
    if (gcLinks) flags |= 1 << 2;
    return _leBytes(flags, 4);
  }
}

/// Options for posting a blob.
class PostOpts {
  const PostOpts({this.salt});

  final CID? salt;
}

/// Options for getting a blob.
class GetOpts {
  const GetOpts({this.salt, this.skipVerify = false});

  final CID? salt;
  final bool skipVerify;
}

/// A content-addressed hash algorithm name.
typedef HashAlgo = String;

/// A schema specification.
class SchemaSpec {
  const SchemaSpec({this.name = '', this.params});

  final String name;
  final Map<String, dynamic>? params;

  Map<String, dynamic> toJson() => {
        'name': name,
        if (params != null) 'params': params,
      };
}

/// Local volume backend configuration.
class VolumeBackendLocal {
  const VolumeBackendLocal({
    required this.schema,
    required this.hashAlgo,
    required this.maxSize,
    this.salted = false,
  });

  final SchemaSpec schema;
  final HashAlgo hashAlgo;
  final int maxSize;
  final bool salted;

  Map<String, dynamic> toJson() => {
        'schema': schema.toJson(),
        'hash_algo': hashAlgo,
        'max_size': maxSize,
        'salted': salted,
      };
}

/// A volume specification.
class VolumeSpec {
  const VolumeSpec({this.local, this.salted = false});

  final VolumeBackendLocal? local;
  final bool salted;

  Map<String, dynamic> toJson() => {'local': local?.toJson()};
}

/// Information returned when inspecting a handle.
class HandleInfo {
  const HandleInfo({
    required this.oid,
    required this.rights,
    required this.createdAt,
    required this.expiresAt,
  });

  final OID oid;
  final ActionSet rights;
  final dynamic createdAt;
  final dynamic expiresAt;
}

/// Information returned when inspecting a volume.
class VolumeInfo {
  const VolumeInfo({required this.id, required this.hashAlgo});

  final OID id;
  final HashAlgo hashAlgo;
}

/// Information returned when inspecting a transaction.
class TxInfo {
  const TxInfo({
    required this.id,
    required this.volume,
    required this.maxSize,
    required this.hashAlgo,
  });

  final OID id;
  final OID volume;
  final int maxSize;
  final HashAlgo hashAlgo;
}

/// In-memory queue backend configuration.
class QueueBackendMemory {
  const QueueBackendMemory({
    required this.maxDepth,
    this.evictOldest = false,
    required this.maxBytesPerMessage,
    required this.maxHandlesPerMessage,
  });

  final int maxDepth;
  final bool evictOldest;
  final int maxBytesPerMessage;
  final int maxHandlesPerMessage;

  Map<String, dynamic> toJson() => {
        'max_depth': maxDepth,
        'evict_oldest': evictOldest,
        'max_bytes_per_message': maxBytesPerMessage,
        'max_handles_per_message': maxHandlesPerMessage,
      };
}

/// A queue specification.
class QueueSpec {
  const QueueSpec({this.memory});

  final QueueBackendMemory? memory;

  Map<String, dynamic> toJson() => {'memory': memory?.toJson()};
}

/// Options for dequeuing messages.
class DequeueOpts {
  const DequeueOpts({
    this.min = 0,
    this.leaveIn = false,
    this.skip = 0,
    this.maxWait,
  });

  final int min;
  final bool leaveIn;
  final int skip;
  final int? maxWait;

  Map<String, dynamic> toJson() => {
        'min': min,
        'leave_in': leaveIn,
        'skip': skip,
        if (maxWait != null) 'max_wait': maxWait,
      };
}

/// A message exchanged through a queue.
class Message {
  const Message({required this.handles, required this.bytes});

  final List<Handle> handles;
  final Uint8List bytes;

  Map<String, dynamic> toJson() => {
        'handles': handles.map((h) => h.toString()).toList(),
        'bytes': base64.encode(bytes),
      };

  /// Marshals this message to its binary form: handle count + handles + bytes.
  List<int> marshal() => [
        ..._leBytes(handles.length, 4),
        for (final h in handles) ...h.marshal(),
        ...bytes,
      ];

  /// Unmarshals a message from its binary form.
  static Message unmarshal(List<int> data) {
    if (data.length < 4) {
      throw BlobcacheException('invalid message: missing handle count');
    }
    final n = _leBytesToInt(data.sublist(0, 4));
    var idx = 4;
    final handles = <Handle>[];
    for (var i = 0; i < n; i++) {
      if (idx + handleSize > data.length) {
        throw BlobcacheException('invalid message: truncated handle list');
      }
      handles.add(Handle.unmarshal(data.sublist(idx, idx + handleSize)));
      idx += handleSize;
    }
    return Message(handles: handles, bytes: Uint8List.fromList(data.sublist(idx)));
  }
}

/// Result of enqueueing messages.
class InsertResp {
  const InsertResp({required this.success});

  final int success;

  static InsertResp fromJson(Map<String, dynamic> json) =>
      InsertResp(success: json['success'] as int);
}

String _hexEncode(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

List<int> _hexDecode(String s) {
  if (s.length.isOdd) {
    throw BlobcacheException('invalid hex string: $s');
  }
  final out = <int>[];
  for (var i = 0; i < s.length; i += 2) {
    out.add(int.parse(s.substring(i, i + 2), radix: 16));
  }
  return out;
}

int _leBytesToInt(List<int> bytes) {
  var value = 0;
  for (var i = bytes.length - 1; i >= 0; i--) {
    value = (value << 8) | bytes[i];
  }
  return value;
}

/// Encodes [value] as [n] little-endian bytes.
List<int> _leBytes(int value, int n) {
  final out = <int>[];
  for (var i = 0; i < n; i++) {
    out.add((value >> (i * 8)) & 0xff);
  }
  return out;
}

String _cidEncode(List<int> bytes) {
  // Order-preserving base64 without padding.
  final standard = base64.encode(Uint8List.fromList(bytes));
  final sb = StringBuffer();
  for (final ch in standard.codeUnits) {
    if (ch == 0x3d) break; // '='
    final idx = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
        .indexOf(String.fromCharCode(ch));
    sb.write(_cidAlphabet[idx]);
  }
  return sb.toString();
}

List<int> _cidDecode(String s) {
  final sb = StringBuffer();
  for (final ch in s.codeUnits) {
    final idx = _cidAlphabet.indexOf(String.fromCharCode(ch));
    if (idx < 0) {
      throw BlobcacheException('invalid cid character: ${String.fromCharCode(ch)}');
    }
    sb.write(
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'[idx]);
  }
  while (sb.length % 4 != 0) {
    sb.write('=');
  }
  return base64.decode(sb.toString());
}