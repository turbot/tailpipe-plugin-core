TAILPIPE_INSTALL_DIR ?= ~/.tailpipe
BUILD_TAGS = netgo

PLUGIN_DIR = $(TAILPIPE_INSTALL_DIR)/plugins/hub.tailpipe.io/plugins/turbot/core@latest
PLUGIN_BINARY = $(PLUGIN_DIR)/tailpipe-plugin-core.plugin
VERSION_JSON = $(PLUGIN_DIR)/version.json
VERSIONS_JSON = $(TAILPIPE_INSTALL_DIR)/plugins/versions.json

.PHONY: install
install:
	go build -o $(PLUGIN_BINARY) -tags "${BUILD_TAGS}" *.go
	$(PLUGIN_BINARY) metadata > $(VERSION_JSON)
	rm -f $(VERSIONS_JSON)

.PHONY: release
release:
	@if [ ! -f ".release-env" ]; then \
		echo ".release-env is required for release";\
		exit 1;\
	fi
	docker run \
		--rm \
		-e CGO_ENABLED=1 \
		--env-file .release-env \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v `pwd`:/go/src/plugin \
		-w /go/src/plugin \
		ghcr.io/goreleaser/goreleaser-cross:${GOLANG_CROSS_VERSION} \
		release --clean --skip=validate