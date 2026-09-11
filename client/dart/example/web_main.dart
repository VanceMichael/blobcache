// Compile-only web entrypoint used to verify that the package (and in
// particular HttpClient) imports cleanly under dart2js/ddc, i.e. that no
// dart:io or dart:ffi code leaks through the public API.
//
// Compile check (does not need to run in a browser):
//   dart compile js -o build/out/web_main.js example/web_main.dart

import 'package:blobcache/blobcache.dart';

Future<void> main() async {
  // Common use case: an HttpClient pointed at a URL relative to the loaded
  // page (the caller supplies a fully-formed base URL).
  final HttpClient client = HttpClient('https://example.com');

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
  await client.abort(tx);
}