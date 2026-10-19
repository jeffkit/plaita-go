GOPROXY ?= https://goproxy.cn,direct

.PHONY: test vet lint build

build:
	GOFLAGS=-mod=mod GOPROXY=$(GOPROXY) go build ./...

test:
	GOFLAGS=-mod=mod GOPROXY=$(GOPROXY) go test ./...

vet:
	GOFLAGS=-mod=mod GOPROXY=$(GOPROXY) go vet ./...

lint: vet
	gofmt -l .
