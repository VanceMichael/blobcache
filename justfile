
# Build the blobcache binary for the current platform.
build: capnp
	mkdir -p ./build/out
	./build/go_exec.sh ./build/out/blobcache ./cmd/blobcache
	./build/go_exec.sh ./build/out/git-remote-bc ./cmd/git-remote-bc

# Build the blobcache binary for amd64 linux.
build-amd64-linux: capnp
	mkdir -p ./build/out
	GOARCH=amd64 GOOS=linux ./build/go_exec.sh ./build/out/blobcache_amd64-linux ./cmd/blobcache
	GOARCH=amd64 GOOS=linux ./build/go_exec.sh ./build/out/git-remote-bc_amd64-linux ./cmd/git-remote-bc

build-arm64-linux: capnp
    mkdir -p ./build/out
    GOARCH=arm64 GOOS=linux ./build/go_exec.sh ./build/out/blobcache_arm64-linux ./cmd/blobcache
    GOARCH=arm64 GOOS=linux ./build/go_exec.sh ./build/out/git-remote-bc_arm64-linux ./cmd/git-remote-bc

build-arm64-darwin: capnp
	mkdir -p ./build/out
	GOARCH=arm64 GOOS=darwin ./build/go_exec.sh ./build/out/blobcache_arm64-darwin ./cmd/blobcache
	GOARCH=arm64 GOOS=darwin ./build/go_exec.sh ./build/out/git-remote-bc_arm64-darwin ./cmd/git-remote-bc

build-exec: build-amd64-linux build-arm64-linux build-arm64-darwin

test-go: capnp
	go test ./...

test-rs: build
	cd ./client/rs && cargo test

test-dart: build
	cd ./client/dart && dart pub get && dart analyze && dart test

# Verify the HttpClient package compiles for the web (no dart:io/dart:ffi
# leak through the public API).
test-dart-web: build
	cd ./client/dart && dart pub get && dart compile js -o build/out/web_main.js example/web_main.dart

# Build an image that contains the Dart SDK and the blobcache daemon binary,
# then run the Dart client tests inside it.
build-dart-image:
	podman build -f client/dart/Dockerfile -t blobcache-dart-test .

test-dart-podman: build-dart-image
	podman run --rm blobcache-dart-test

test:
	just test-go
	just test-rs
	just test-zig
	just test-dart
	just test-dart-web

testv:
	go test -count=1 -v ./pkg/...

install-capnpc-go:
	GOBIN="${GOBIN:-$(go env GOPATH)/bin}" go install capnproto.org/go/capnp/v3/capnpc-go@latest

capnp: install-capnpc-go
	PATH="$(go env GOPATH)/bin:${PATH}"; cd ./src/internal/tries/triescnp && ./build.sh

clean:
	rm -f ./build/out/*
	./build/rm_images.sh

build-images: build-amd64-linux
	./build/build_images.sh

cargo-publish:
	cargo publish --manifest-path ./client/rs/Cargo.toml

release-build:
    just build-exec
    just build-images

release-publish:
	./build/push_images.sh
	just cargo-publish

build-zig:
	cd ./client/zig && zig build

test-zig: build
	cd ./client/zig && zig build test --summary all

build-dart:
	cd ./client/dart && dart pub get && dart analyze

# Installs just the blobcache binary to /usr/bin/blobcache
install-unix: build
	sudo cp ./build/out/blobcache /usr/bin/blobcache
	sudo cp ./build/out/git-remote-bc /usr/bin/git-remote-bc

# Install blobcache with systemd service
install-systemd: build
	./etc/install-systemd.sh
