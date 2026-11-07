VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)
GO ?= go

# Install location: user-local by default, no sudo required.
# Override with e.g. `make install PREFIX=/usr/local` or
# `make install PREFIX=$HOME/.local DESTDIR=/tmp/stage`.
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin

.PHONY: build test check cross release platforms clean install uninstall

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/catsync ./cmd/catsync

test:
	$(GO) test ./...

check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	$(GO) vet ./...
	$(GO) test -race ./...

# Every platform the release workflow publishes.
PLATFORMS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

platforms:
	@echo $(PLATFORMS)

# cross builds one plain binary per platform into dist/.
cross:
	@for target in $(PLATFORMS); do \
	  os=$${target%/*}; arch=$${target#*/}; \
	  echo "GOOS=$$os GOARCH=$$arch go build -o dist/catsync-$$os-$$arch"; \
	  GOOS=$$os GOARCH=$$arch $(GO) build -ldflags "$(LDFLAGS)" \
	    -o dist/catsync-$$os-$$arch ./cmd/catsync || exit 1; \
	done

# release builds the same binaries into versioned, checksummed archives that the
# GitHub release workflow attaches to a tag. It works on macOS and Linux: the
# checksum tool differs between them, so pick whichever exists.
release:
	@mkdir -p dist/release
	@for target in $(PLATFORMS); do \
	  os=$${target%/*}; arch=$${target#*/}; \
	  name="catsync_$(VERSION)_$${os}_$${arch}"; \
	  dir="dist/release/$$name"; \
	  mkdir -p "$$dir"; \
	  echo "building $$name"; \
	  GOOS=$$os GOARCH=$$arch $(GO) build -ldflags "$(LDFLAGS)" -o "$$dir/catsync" ./cmd/catsync || exit 1; \
	  cp LICENSE README.md "$$dir/" 2>/dev/null || cp LICENSE "$$dir/"; \
	  tar -czf "dist/release/$$name.tar.gz" -C "dist/release" "$$name" || exit 1; \
	  rm -rf "$$dir"; \
	done
	@cd dist/release && (command -v sha256sum >/dev/null && sha256sum *.tar.gz > SHA256SUMS || shasum -a 256 *.tar.gz > SHA256SUMS)
	@echo "release archives:"; ls -1 dist/release

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
