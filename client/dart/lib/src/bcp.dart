// BCP wire protocol encoding helpers.

import 'dart:convert';
import 'dart:typed_data';

import 'types.dart';

const int headerLen = 8;

typedef MessageCode = int;

const int _sectionSize = 256;

const int mtEndpoint = 2;
const int mtInspect = 3;
const int mtOpenFiat = 4;

const int mtHandleInspect = (1 * _sectionSize) + 0;
const int mtHandleDrop = (1 * _sectionSize) + 1;
const int mtHandleKeepAlive = (1 * _sectionSize) + 2;
const int mtHandleShareOut = (1 * _sectionSize) + 3;
const int mtHandleShareIn = (1 * _sectionSize) + 4;

const int mtVolumeInspect = (2 * _sectionSize) + 0;
const int mtVolumeBeginTx = (2 * _sectionSize) + 1;
const int mtOpenFrom = (2 * _sectionSize) + 2;
const int mtCreateVolume = (3 * _sectionSize) - 1;

const int mtTxInspect = (3 * _sectionSize) + 0;
const int mtTxAbort = (3 * _sectionSize) + 1;
const int mtTxCommit = (3 * _sectionSize) + 2;
const int mtTxLoad = (3 * _sectionSize) + 3;
const int mtTxSave = (3 * _sectionSize) + 4;
const int mtTxPost = (3 * _sectionSize) + 5;
const int mtTxPostSalt = (3 * _sectionSize) + 6;
const int mtTxGet = (3 * _sectionSize) + 7;
const int mtTxGetSalt = (3 * _sectionSize) + 8;
const int mtTxExists = (3 * _sectionSize) + 9;
const int mtTxDelete = (3 * _sectionSize) + 10;
const int mtTxCopy = (3 * _sectionSize) + 11;
const int mtTxLink = (3 * _sectionSize) + 12;
const int mtTxUnlink = (3 * _sectionSize) + 13;
const int mtTxVisit = (3 * _sectionSize) + 14;
const int mtTxIsVisited = (3 * _sectionSize) + 15;
const int mtTxVisitLinks = (3 * _sectionSize) + 16;

const int mtQueueInspect = (4 * _sectionSize) + 0;
const int mtQueueEnqueue = (4 * _sectionSize) + 1;
const int mtQueueDequeue = (4 * _sectionSize) + 2;
const int mtQueueSubToVolume = (4 * _sectionSize) + 3;
const int mtQueueCreate = (5 * _sectionSize) - 1;

const int mtOk = (255 * _sectionSize) + 0;
const int mtErrorTimeout = (255 * _sectionSize) + 1;
const int mtErrorInvalidHandle = (255 * _sectionSize) + 2;
const int mtErrorNotFound = (255 * _sectionSize) + 3;
const int mtErrorNoPermission = (255 * _sectionSize) + 4;
const int mtErrorNoLink = (255 * _sectionSize) + 5;
const int mtErrorTooLarge = (255 * _sectionSize) + 6;
const int mtErrorUnknown = 0xffff;

/// Encodes a BCP message into a header + body byte sequence.
List<int> writeMessage(MessageCode code, List<int> body) {
  final header = Uint8List(headerLen);
  header[0] = (code >> 8) & 0xff;
  header[1] = code & 0xff;
  final len = body.length;
  header[4] = len & 0xff;
  header[5] = (len >> 8) & 0xff;
  header[6] = (len >> 16) & 0xff;
  header[7] = (len >> 24) & 0xff;
  return [...header, ...body];
}

/// Checks a response code, throwing on any error code.
void checkOk(MessageCode code, List<int> body) {
  if (code == mtOk) return;
  final msg = utf8.decode(body, allowMalformed: true);
  if (code == mtErrorTimeout ||
      code == mtErrorInvalidHandle ||
      code == mtErrorNotFound ||
      code == mtErrorNoPermission ||
      code == mtErrorNoLink ||
      code == mtErrorTooLarge ||
      code == mtErrorUnknown) {
    throw BlobcacheException('wire error $code: $msg');
  }
  throw BlobcacheException('unexpected response code $code: $msg');
}

/// Appends an unsigned varint to [out].
void writeUvarint(BytesBuilder out, int x) {
  var v = x;
  while (v >= 0x80) {
    out.addByte((v & 0xff) | 0x80);
    v >>= 7;
  }
  out.addByte(v);
}

/// Reads an unsigned varint from [data] starting at [offset], returning the
/// value and the byte offset just past it.
(int, int) readUvarint(List<int> data, int offset) {
  var x = 0;
  var s = 0;
  var i = offset;
  while (true) {
    if (i >= data.length) {
      throw BlobcacheException('truncated uvarint');
    }
    final b = data[i];
    i++;
    if (b < 0x80) {
      if ((i - offset) > 10 || ((i - offset) == 10 && b > 1)) {
        throw BlobcacheException('uvarint overflow');
      }
      return (x | (b << s), i);
    }
    x |= (b & 0x7f) << s;
    s += 7;
  }
}

/// Decodes a bit-packed boolean vector of [n] entries.
List<bool> decodeBools(List<int> data, int n) {
  final out = <bool>[];
  for (final byte in data) {
    for (var bit = 0; bit < 8; bit++) {
      if (out.length == n) return out;
      out.add((byte & (1 << bit)) != 0);
    }
  }
  while (out.length < n) {
    out.add(false);
  }
  return out;
}

/// Appends a length-prefixed payload to [out].
void appendLp(BytesBuilder out, List<int> data) {
  writeUvarint(out, data.length);
  out.add(data);
}

/// Reads a length-prefixed payload from [data] starting at [offset],
/// returning the payload and the offset just past it.
(List<int>, int) readLp(List<int> data, int offset) {
  final (len, off) = readUvarint(data, offset);
  if (off + len > data.length) {
    throw BlobcacheException('truncated lp payload');
  }
  return (data.sublist(off, off + len), off + len);
}