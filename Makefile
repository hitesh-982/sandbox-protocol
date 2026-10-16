.PHONY: all build test test-go test-py clean run-pm run-agent

all: build test

build:
	@mkdir -p bin
	go build -o bin/pool-manager ./cmd/pool-manager
	go build -o bin/sandbox-agent ./cmd/sandbox-agent

test: test-go test-py

test-go:
	go test -v ./pkg/...

test-py: build
	python3 -m pytest -v tests/

run-pm: build
	./bin/pool-manager -addr=127.0.0.1:8080 -node-id=node-local -warm-count=2

run-agent: build
	./bin/sandbox-agent -addr=127.0.0.1:9999 -dir=/tmp

clean:
	rm -rf bin/ .pytest_cache/
