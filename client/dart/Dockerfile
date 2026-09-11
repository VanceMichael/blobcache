# Builds a blobcache daemon binary and runs the Dart client tests against it.
# Build from the repository root:
#   podman build -f client/dart/Dockerfile -t blobcache-dart-test .
#
# The Dart test (client/dart/test/daemon_ephemeral_test.dart) locates the
# daemon binary at ../../build/out/blobcache relative to the test's working
# directory, so the binary is placed at /app/build/out/blobcache and tests are
# run from /app/client/dart.

# Stage 1: build the blobcache daemon binary.
FROM docker.io/library/golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath \
        -ldflags "-extldflags '-static'" \
        -o /blobcache ./cmd/blobcache

# Stage 2: run the Dart client tests against the daemon.
FROM docker.io/dart:stable

RUN mkdir -p /app/build/out /app/client/dart
COPY --from=builder /blobcache /app/build/out/blobcache
COPY client/dart/ /app/client/dart/

WORKDIR /app/client/dart
RUN dart pub get

# Lint, verify the package compiles for the web, then run the tests against
# the daemon.
CMD ["sh", "-c", "dart analyze && dart compile js -o build/out/web_main.js example/web_main.dart && dart test"]