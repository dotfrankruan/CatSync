VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)
GO ?= go

# Install location: user-local by default, no sudo required.
# Override with e.g. `make install PREFIX=/usr/local` or
# `make install PREFIX=$HOME/.local DESTDIR=/tmp/stage`.
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin

.PHONY: build test check cross clean install uninstall

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

# install builds and copies the binary into BINDIR (default
# $(PREFIX)/bin = ~/.local/bin). The directory is created if needed and the
# file is replaced atomically by install(1).
install: build
	@mkdir -p "$(DESTDIR)$(BINDIR)"
	install -m 0755 bin/catsync "$(DESTDIR)$(BINDIR)/catsync"
	@echo "installed $(DESTDIR)$(BINDIR)/catsync"
	@case ":$$PATH:" in *":$(BINDIR):"*) ;; \
	  *) echo "note: $(BINDIR) is not in PATH; add it with:"; \
	     echo "      export PATH=\"$(BINDIR):$$PATH\"";; esac

uninstall:
	rm -f "$(DESTDIR)$(BINDIR)/catsync"
	@echo "removed $(DESTDIR)$(BINDIR)/catsync"
