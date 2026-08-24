GO ?= go
export GOTOOLCHAIN := local

test:
	$(GO) test ./... -count=1
race:
	$(GO) test -race ./... -count=1
vet:
	$(GO) vet ./...
build:
	$(GO) build ./...
docker-build:
	docker build --platform linux/amd64 -t typhoon-relief-g10:dev .
