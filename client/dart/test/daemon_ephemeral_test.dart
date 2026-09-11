import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:blobcache/blobcache.dart';
import 'package:test/test.dart';

/// Path to the built blobcache binary, relative to the package root.
String _daemonPath() => '${Directory.current.path}/../../build/out/blobcache';

String _uniqueSocketPath() {
  final nanos = DateTime.now().microsecondsSinceEpoch;
  return '/tmp/blobcache-dart-test-$pid-$nanos.sock';
}

void main() {
  test('daemon ephemeral create volume and edit transaction', () async {
    final socketPath = _uniqueSocketPath();

    final daemon = await Process.start(
      _daemonPath(),
      ['daemon', 'ephemeral', '--serve-ipc', socketPath, '--net', '127.0.0.1:0'],
      mode: ProcessStartMode.detachedWithStdio,
    );

    try {
      await _waitForSocket(socketPath);

      final client = Client('unix://$socketPath');

      final volume = await client.createVolume(
        null,
        VolumeSpec(
          local: VolumeBackendLocal(
            schema: const SchemaSpec(),
            hashAlgo: 'blake3-256',
            maxSize: 1 << 20,
          ),
        ),
      );

      final tx = await client.beginTx(volume, const TxParams(modify: true));

      await client.save(tx, utf8.encode('hello from dart'));
      final loaded = await client.load(tx);
      expect(utf8.decode(loaded), 'hello from dart');

      final cid = await client.post(tx, utf8.encode('blob-data'), const PostOpts());
      final got = await client.get(tx, cid, const GetOpts());
      expect(utf8.decode(got), 'blob-data');

      await client.commit(tx);
    } finally {
      daemon.kill();
      try {
        await File(socketPath).delete();
      } on FileSystemException {
        // Socket may already be gone.
      }
    }
  });
}

Future<void> _waitForSocket(String path) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  while (DateTime.now().isBefore(deadline)) {
    if (await File(path).exists()) return;
    await Future.delayed(const Duration(milliseconds: 50));
  }
  throw TimeoutException('daemon did not become ready: $path');
}