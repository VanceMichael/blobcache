// Web fallback for the unix-socket client builder.
//
// Unix domain sockets (and the dart:ffi/dart:io they require) are not
// available in the browser, so this stub compiles the feature out.

import 'service.dart';

/// Always throws: the unix-socket transport is not supported on the web.
Service buildUnixClient(String path) =>
    throw UnsupportedError('unix transport is not supported on the web');