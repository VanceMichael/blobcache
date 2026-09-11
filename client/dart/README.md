# Blobcache Dart Client

A Dart client for the [Blobcache](https://blobcache.io) API, supporting both the
unix-socket (BCP) transport and the HTTP+JSON transport.

- `HttpClient` — HTTP+JSON, runs on the VM and in the browser (same-origin).
- `UNIXClient` — BCP over a unix domain socket (VM only; not available on web).

## Using from another project

The package is not published to pub.dev yet. Consumers can depend on it
directly from the public GitHub mirror, selecting the `client/dart`
subdirectory:

```yaml
# another project's pubspec.yaml
dependencies:
  blobcache:
    git:
      url: https://github.com/blobcache/blobcache.git
      ref: master          # branch, tag, or commit
      path: client/dart
```

Then import it as usual:

```dart
import 'package:blobcache/blobcache.dart';
```

## Quick start

```dart
final client = Client('unix:///run/blobcache/blobcache.sock');

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
final root = await client.load(tx);
await client.commit(tx);
```

For a browser, construct an `HttpClient` pointed at the blobcache server
(served same-origin relative to the loaded page):

```dart
final client = HttpClient('https://example.com');
```

## Endpoint selection

`Client(endpoint)` dispatches on the URL scheme:

- `unix://…` → `UNIXClient`
- anything else → `HttpClient` (e.g. `http://host:port` or `https://…`)

`Client.fromEnv()` reads the `BLOBCACHE_API` environment variable, falling back
to `unix:///run/blobcache/blobcache.sock` (VM only).

## Development

```sh
dart pub get
dart analyze
dart test              # runs an ephemeral-daemon integration test
dart compile js -o build/out/web_main.js example/web_main.dart  # web import check
```

The integration test spawns a locally built `blobcache` daemon binary
(`../../build/out/blobcache`), so run `just build` from the repository root
first. See the repository's `justfile` for the `test-dart` and `test-dart-web`
recipes.

## License

Blobcache clients are licensed under MPL 2.0. See the repository's `LICENSE`
for details.