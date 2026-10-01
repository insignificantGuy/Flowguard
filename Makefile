.PHONY: build run test vet lint fmt clean docker

VERSION ?= 1.0.0

build:
	go build -ldflags="-s -w -X main.version=$(VERSION)" -trimpath -o bin/flowguard ./cmd/server

run:
	go run ./cmd/server

test:
	go test ./... -v -race -count=1

vet:
	go vet ./...

fmt:
	gofmt -w ./cmd ./internal ./test

clean:
	rm -rf bin dist

docker:
	docker build -t flowguard:$(VERSION) .