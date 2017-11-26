NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

IGNORED_PACKAGES := /vendor/

.PHONY: all deps test

all: deps test

deps:
	@echo "$(OK_COLOR)==> Installing glide dependencies$(NO_COLOR)"
	@go get -u github.com/golang/dep/cmd/dep
	@dep ensure

test:
	@/bin/sh -c "./build/test.sh $(allpackages)"

_allpackages = $(shell ( go list ./... 2>&1 1>&3 | \
    grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)) 1>&2 ) 3>&1 | \
    grep -v -e "^$$" $(addprefix -e ,$(IGNORED_PACKAGES)))

allpackages = $(if $(__allpackages),,$(eval __allpackages := $$(_allpackages)))$(__allpackages)
