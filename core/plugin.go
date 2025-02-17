package core

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log/slog"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-core/sources/file"
	"github.com/turbot/tailpipe-plugin-core/tables/log"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

func init() {
	// register sources
	row_source.RegisterRowSource[*file.FileSource]()
}

const PluginName = "core"

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

// Collect overrides the Collect method in PluginImpl - we do this to parse the format
// which is used to register the custom table
func (p *Plugin) Collect(ctx context.Context, req *proto.CollectRequest) (*row_source.ResolvedFromTime, *schema.RowSchema, error) {
	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	slog.Info("Collect - core plugin")

	collectRequest, err := types.CollectRequestFromProto(req)
	if err != nil {
		slog.Error("CollectRequestFromProto failed", "error", err)

		return nil, nil, err
	}
	// we expect the request to contain a custom table name, as this plugin only provides custom tables
	// validate there is a table and that is has a format
	if err = p.validateRequest(collectRequest); err != nil {
		return nil, nil, err
	}
	// map req to our internal type
	// parse the format
	format, err := formats.ParseFormat(collectRequest.SourceFormat)
	// we need to register a collector in the table factory for the custom table name
	// this is so that the table factory can create the collector when it is needed
	table.RegisterCustomTable[*table.DynamicRow, *log.LogTable](collectRequest.CustomTableDef, format)

	// now call the base implementation of Collect
	return p.PluginImpl.DoCollect(ctx, collectRequest)
}

// validate there is a table and that is has a format
func (p *Plugin) validateRequest(req *types.CollectRequest) error {
	if req.CustomTableDef == nil {
		return fmt.Errorf("custom table is required")
	}
	if req.CustomTableDef.Name == "" {
		return fmt.Errorf("custom table name is required")
	}
	if req.SourceFormat == nil {
		return fmt.Errorf("source format is required")
	}
	return nil
}
