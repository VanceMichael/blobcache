// Web fallback for the environment read helper.
//
// The browser has no process environment, so this is compiled out on web and
// returns null (which resolves to the default endpoint).

/// Always returns null: the web platform has no process environment.
String? readEnvironment(String name) => null;