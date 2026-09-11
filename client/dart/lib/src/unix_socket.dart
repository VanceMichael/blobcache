// Minimal unix domain stream socket client backed by libc via dart:ffi.
//
// dart:io does not expose unix domain sockets, so this provides just enough
// to open a SOCK_STREAM client connection and do exact-length reads/writes.

import 'dart:convert';
import 'dart:ffi';
import 'dart:io' show Platform;
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'types.dart';

final DynamicLibrary _libc = DynamicLibrary.process();

typedef _SocketNative = Int32 Function(Int32 domain, Int32 type, Int32 protocol);
typedef _SocketDart = int Function(int domain, int type, int protocol);
typedef _ConnectNative = Int32 Function(
    Int32 sockfd, Pointer<SockAddrUn> addr, Uint32 addrlen);
typedef _ConnectDart = int Function(int sockfd, Pointer<SockAddrUn> addr, int addrlen);
typedef _ReadNative = Int64 Function(Int32 fd, Pointer<Void> buf, Uint64 count);
typedef _ReadDart = int Function(int fd, Pointer<Void> buf, int count);
typedef _WriteNative = Int64 Function(Int32 fd, Pointer<Void> buf, Uint64 count);
typedef _WriteDart = int Function(int fd, Pointer<Void> buf, int count);
typedef _CloseNative = Int32 Function(Int32 fd);
typedef _CloseDart = int Function(int fd);

final _socketFn = _libc.lookupFunction<_SocketNative, _SocketDart>('socket');
final _connectFn = _libc.lookupFunction<_ConnectNative, _ConnectDart>('connect');
final _readFn = _libc.lookupFunction<_ReadNative, _ReadDart>('read');
final _writeFn = _libc.lookupFunction<_WriteNative, _WriteDart>('write');
final _closeFn = _libc.lookupFunction<_CloseNative, _CloseDart>('close');

int _getErrno() {
  if (Platform.isLinux) {
    final f = _libc.lookupFunction<Pointer<Int32> Function(),
        Pointer<Int32> Function()>('__errno_location');
    return f().value;
  }
  if (Platform.isMacOS) {
    final f = _libc.lookupFunction<Pointer<Int32> Function(),
        Pointer<Int32> Function()>('__error');
    return f().value;
  }
  return 0;
}

const int _afUnix = 1; // AF_UNIX on Linux/macOS
const int _sockStream = 1; // SOCK_STREAM

// struct sockaddr_un { sa_family_t sun_family; char sun_path[108]; }
final class SockAddrUn extends Struct {
  @Uint16()
  external int sunFamily;

  @Array(108)
  external Array<Uint8> sunPath;
}

/// A connected unix domain stream socket.
class UnixDomainSocket {
  UnixDomainSocket._(this._fd);

  final int _fd;
  bool _closed = false;

  static UnixDomainSocket connect(String path) {
    final name = utf8.encode(path);
    if (name.length >= 108) {
      throw BlobcacheException('unix socket path too long: $path');
    }
    final fd = _socketFn(_afUnix, _sockStream, 0);
    if (fd < 0) {
      throw BlobcacheException('socket() failed: errno ${_getErrno()}');
    }
    try {
      final addr = calloc<SockAddrUn>();
      try {
        addr.ref.sunFamily = _afUnix;
        final pathBytes = addr.ref.sunPath;
        for (var i = 0; i < name.length; i++) {
          pathBytes[i] = name[i];
        }
        pathBytes[name.length] = 0;
        final rc = _connectFn(fd, addr, sizeOf<SockAddrUn>());
        if (rc != 0) {
          throw BlobcacheException('connect() failed: errno ${_getErrno()}');
        }
      } finally {
        calloc.free(addr);
      }
    } catch (_) {
      _closeFn(fd);
      rethrow;
    }
    return UnixDomainSocket._(fd);
  }

  void write(List<int> data) {
    if (_closed) throw StateError('socket closed');
    final bytes = Uint8List.fromList(data);
    final ptr = malloc<Uint8>(bytes.length);
    try {
      ptr.asTypedList(bytes.length).setAll(0, bytes);
      var written = 0;
      while (written < bytes.length) {
        final n = _writeFn(_fd, (ptr + written).cast(), bytes.length - written);
        if (n < 0) {
          throw BlobcacheException('write() failed: errno ${_getErrno()}');
        }
        written += n;
      }
    } finally {
      malloc.free(ptr);
    }
  }

  /// Reads up to [count] bytes; returns fewer only on EOF.
  Uint8List read(int count) {
    if (_closed) throw StateError('socket closed');
    final out = BytesBuilder();
    final ptr = malloc<Uint8>(count);
    try {
      var remaining = count;
      while (remaining > 0) {
        final n = _readFn(_fd, ptr.cast(), remaining);
        if (n < 0) {
          throw BlobcacheException('read() failed: errno ${_getErrno()}');
        }
        if (n == 0) break; // EOF
        out.add(ptr.asTypedList(n));
        remaining -= n;
      }
    } finally {
      malloc.free(ptr);
    }
    return out.takeBytes();
  }

  void close() {
    if (_closed) return;
    _closed = true;
    _closeFn(_fd);
  }
}