.PHONY: build test check clean

build:
	go build -o bin/catsync ./cmd/catsync

test:
	go test ./...

check:
	gofmt -l . | grep -q . && exit 1 || true
	go vet ./...
	go test -race ./...

clean:
	rm -rf bin dist
