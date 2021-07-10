
BINARY=example
VERSION=$(shell cat VERSION)
BUILD_ID := $(shell git rev-parse --short HEAD)

LDFLAGS=-ldflags "-X=main.Version=$(VERSION) -X=main.Build=$(BUILD_ID)"

all: build

deps:
	go get -u github.com/cespare/reflex
	go get -u github.com/rakyll/gotest
	go mod download

build:
	go build -v $(LDFLAGS) -o ${BINARY} ./cmd/main.go

dev:
	go run -v $(LDFLAGS) ./example/pubsub/*.go

watch-dev: deps
	reflex -t 50ms -s -- sh -c 'echo \\nBUILDING && make dev && echo Exited \(0\)'

test:
	gotest -v ./tests/...

watch-test: deps
	reflex -t 50ms -s -- sh -c 'make test'

clean:
	rm -f ${BINARY}

re: clean all
