DATABASE_URL ?= postgres://postgres@localhost:5432/deployd_test?sslmode=disable

.PHONY: build test test-db lint generate run docker helm-lint

build:
	go build -o bin/deployd ./cmd/deployd

## Unit tests only (database tests skip without DATABASE_URL).
test:
	go test -race ./...

## All tests, including PostgreSQL integration tests.
test-db:
	DATABASE_URL="$(DATABASE_URL)" go test -race -count=1 ./...

lint:
	go vet ./...
	buf lint

## Regenerate gen/ from proto/ (needs buf, protoc-gen-go, protoc-gen-connect-go).
generate:
	buf generate

run:
	DEPLOYD_TOKENS=dev-viewer:viewer,dev-deployer:deployer go run ./cmd/deployd

docker:
	docker build -t deployd:dev .

helm-lint:
	helm lint deploy/helm/deployd
