all: fmt build lint

GIT_TAG := $(shell git describe --tags --always --dirty)
LDFLAGS := -ldflags="-X 'main.GitTag=$(GIT_TAG)'"

.PHONY: all artifacts build clean fmt lint lint-update publish replace

artifacts:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o msc-posix-linux-amd64 .
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o msc-posix-linux-arm64 .

build:
	go build $(LDFLAGS) .

clean:
	go clean -i .
	rm -f msc-posix-linux-amd64
	rm -f msc-posix-linux-arm64

fmt:
	go fmt .

lint:
	golangci-lint run --config .golangci.yml .

lint-update:
	rm -f $(GOPATH)/bin/golangci-lint
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

publish:
	 (cd ../../.. && docker build --file posix/fuse/mscp/Dockerfile --target built --tag mscp_built:$(GIT_TAG) .)
	 docker create --name mscp_built mscp_built:$(GIT_TAG) --entrypoint /mscp && \
	 docker cp mscp_built:/msc-posix-linux-amd64 ./msc-posix-linux-amd64 && \
	 docker cp mscp_built:/msc-posix-linux-arm64 ./msc-posix-linux-arm64 && \
	 docker rm mscp_built
