# Local gate. `make ci` mirrors .github/workflows/ci.yml step for step, and the
# skipped-test list has one source of truth (scripts/ci-skip.txt) shared with CI.
#
# GOFLAGS is deliberately not forced: `-mod=mod` let `make build` rewrite go.mod/
# go.sum and exit 0 where CI (read-only by default) fails on the same tree.
GOPROXY ?= https://goproxy.cn,direct
SKIP := $(shell cat scripts/ci-skip.txt)

.PHONY: build test test-all vet fmt-check audit lint ci

build:
	GOPROXY=$(GOPROXY) go build ./...

test:
	GOPROXY=$(GOPROXY) go test ./... -count=1 -skip '$(SKIP)'

# Same suite with the CI skips removed — red on a host without egress, which is
# why the skip exists; run it when touching the http node.
test-all:
	GOPROXY=$(GOPROXY) go test ./... -count=1

vet:
	GOPROXY=$(GOPROXY) go vet ./...

# `gofmt -l` exits 0 even when it lists files, so the old `lint` recipe printed
# unformatted paths and passed while CI failed on the very same tree.
fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt would rewrite:"; echo "$$out"; exit 1; fi

audit:
	bash scripts/ci-workflow-audit.sh

lint: fmt-check vet

ci: fmt-check vet build test audit
