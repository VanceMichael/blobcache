// VM (dart:io) implementation of the unix-socket client builder.

import 'service.dart';
import 'unix.dart';

/// Builds a [Service] that talks BCP over a unix domain socket at [path].
Service buildUnixClient(String path) => UNIXClient(path);