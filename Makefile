VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)
GO ?= go

.PHONY: build test check cross clean

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/catsync ./cmd/catsync

test:
	$(GO) test ./...

check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) test -race ./...

cross:
	GOOS=linux GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/catsync-linux-amd64 ./cmd/catsync
	GOOS=linux GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/catsync-linux-arm64 ./cmd/catsync
	GOOS=darwin GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/catsync-darwin-arm64 ./cmd/catsync

clean:
	rm -rf bin dist
