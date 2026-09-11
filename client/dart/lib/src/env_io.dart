// VM (dart:io) implementation of the environment read helper.

import 'dart:io' as io;

/// Reads the value of [name] from the process environment, or null if unset.
String? readEnvironment(String name) => io.Platform.environment[name];