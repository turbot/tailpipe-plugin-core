package core

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-core/formats"
	"github.com/turbot/tailpipe-plugin-core/tables"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"log/slog"

	// reference the table and sources packages to ensure that the tables and sources are registered by the init functions
	_ "github.com/turbot/tailpipe-plugin-core/sources"
	_ "github.com/turbot/tailpipe-plugin-core/tables"
)

const PluginName = "custom"

type Plugin struct {
	plugin.PluginImpl
}

func NewPlugin() (_ plugin.TailpipePlugin, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	p := &Plugin{
		PluginImpl: plugin.NewPluginImpl(PluginName),
	}

	// override the default table factory with out own implementation
	return p, nil
}

// Init Override the Init method in PluginImpl to do nothing
// (we will initialise the table factory in the Collect method, after registering the collector for the custom table)
func (p *Plugin) Init(context.Context) error {
	return nil
}

// Collect overrides the Collect method in PluginImpl - so we do not use the factory to create a collector,
// instead we create our own
func (p *Plugin) Collect(ctx context.Context, req *proto.CollectRequest) (*schema.RowSchema, error) {
	slog.Info("Collect - core plugin")

	// we expect the request to contain a custom table name, as this plugin only provides custom tables
	// validate there is a table and that is has a format
	err := p.validateRequest(req)
	if err != nil {
		return nil, err
	}

	// we need to register a collector in the table factory for the custom table name
	// this is so that the table factory can create the collector when it is needed
	// determine whether we need an ArtifactConversionCollector or a standard CollectorImpl

	var collector table.Collector

	switch req.SourceFormat.Target {
	case constants.SourceFormatCustom:
		slog.Info("Custom source format")
		c := table.NewCollectorWithFormat[*table.DynamicRow, *formats.Custom, *tables.LogTable]()
		// we need to set the name on the table
		c.Table.(*tables.LogTable).Name = req.CustomTable.Name

		collector = c
	case constants.SourceFormatDelimited:
		slog.Info("Delimited source format")
		collector = table.NewArtifactConversionCollector[*formats.Delimited](req.CustomTable.Name, req.SourceFormat)
	//case constants.SourceFormatJsonLines:
	//case constants.SourceFormatJson:
	default:
		return nil, fmt.Errorf("unsupported source format: %s", req.SourceFormat.Target)
	}

	// now we have a collector we can register it with the table factory
	table.RegisterCollector(func() table.Collector { return collector })
	// initialise the factory
	if err := table.Factory.Init(); err != nil {
		return nil, err
	}

	// now call the base implementation of Collect
	return p.PluginImpl.Collect(ctx, req)
}

// validate there is a table and that is has a format
func (p *Plugin) validateRequest(req *proto.CollectRequest) error {
	if req.CustomTable == nil {
		return fmt.Errorf("custom table is required")
	}
	if req.CustomTable.Name == "" {
		return fmt.Errorf("custom table name is required")
	}
	if req.SourceFormat == nil {
		return fmt.Errorf("source format is required")
	}
	return nil
}
