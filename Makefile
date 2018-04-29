.PHONY: all deps test

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
		go test -v -cover integration/rabbus_integration_test.go -bench .
	@docker-compose down -v
