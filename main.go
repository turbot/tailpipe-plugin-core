package main

import (
	"log/slog"
	"os"

	"github.com/turbot/tailpipe-plugin-core/core"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
)

func main() {
	// if the `metadata` arg was passed, we are running in metadata mode - return our metadata
	if len(os.Args) > 1 && os.Args[1] == "metadata" {
		// print the metadata and exit
		os.Exit(plugin.PrintMetadata(core.NewPlugin))
	}

	err := plugin.Serve(&plugin.ServeOpts{
		PluginFunc: core.NewPlugin,
	})

	if err != nil {
		slog.Error("Error starting plugin", "error", err)
	}
}
