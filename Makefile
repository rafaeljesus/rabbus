.PHONY: all deps test

all: deps test

deps:
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure

test:
	@go test -v -race -cover

integration-test:
	@go test -v -race -cover integration/rabbus_integration_test.go -bench .
