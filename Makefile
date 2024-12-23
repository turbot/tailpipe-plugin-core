TAILPIPE_INSTALL_DIR ?= ~/.tailpipe
BUILD_TAGS = netgo
install:
	go build -o $(TAILPIPE_INSTALL_DIR)/plugins/hub.tailpipe.io/plugins/turbot/core@latest/tailpipe-plugin-core.plugin -tags "${BUILD_TAGS}" *.go
