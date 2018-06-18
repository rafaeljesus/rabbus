.PHONY: all deps test integration-test-ci

all: deps test integration-test

deps:
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure

test:
	@go test -v -race -cover

integration-test:
	@docker-compose up -d
	@sleep 3
	AMQP_DSN="amqp://guest:guest@`docker-compose port rabbit 5672`/" \
	AMQP_MANAGEMENT_PORT="http://`docker-compose port rabbit 15672`/api" \
		go test -timeout=30s -v -cover integration/rabbus_integration_test.go -bench .
	@docker-compose down -v

integration-test-ci:
	@go test -v -race -cover integration/rabbus_integration_test.go -bench .
