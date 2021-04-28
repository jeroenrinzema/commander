PROJECT = github.com/jeroenrinzema/commander
VERSION = $(shell git describe --match "v[0-9]*" --abbrev=4 HEAD)
COMMIT 	= $(shell git rev-parse --short HEAD)

BIN 	= $(CURDIR)/bin
BUILD = $(CURDIR)/build

GO = go

PKGS = $(or $(PKG),$(shell env $(GO) list ./...))
LDFLAGS += -X "$(PROJECT)/pkg/buildinfo.version=$(VERSION)" -X "$(PROJECT)/pkg/buildinfo.commit=$(COMMIT)"

# printing
V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")

$(BIN):
	@mkdir -p $@
$(BIN)/%: | $(BIN) ; $(info $(M) building $(PACKAGE)…)
	$Q env GOBIN=$(BIN) $(GO) install $(PACKAGE)

GOLINT = $(BIN)/golangci-lint
$(BIN)/golangci-lint: PACKAGE=github.com/golangci/golangci-lint/cmd/golangci-lint

STRINGER = $(BIN)/stringer
$(BIN)/stringer: PACKAGE=golang.org/x/tools/cmd/stringer

.PHONY: tools
tools: ## Install all used development tools
	$(info $(M) installing tools…)
	@cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % env GOBIN=$(BIN) go install 2> /dev/null %

.PHONY: generate
generate: | $(STRINGER) ; $(info $(M) executing generators…) @ ## Scans all "magic" comments and executes the defined generators
	$Q $(GO) generate -x ./...

.PHONY: lint
lint: | $(GOLINT) ; $(info $(M) running golint…) @ ## Run the project linters
	$Q $(GOLINT) run --max-issues-per-linter 10

.PHONY: test
test: ## Run all tests
	$Q $(GO) test -cover -race ./...

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	$Q $(GO) fmt $(PKGS)

.PHONY: clean
clean: ; $(info $(M) cleaning…)	@ ## Cleanup everything
	@rm -rf $(BIN)
	@rm -rf $(BUILD)

.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'