package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-core/sources/file"
	"github.com/turbot/tailpipe-plugin-core/tables/log"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
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

	return p, nil
}

// Collect overrides the Collect method in PluginImpl - we do this to parse the format
// which is used to register the custom table
func (p *Plugin) Collect(ctx context.Context, req *proto.CollectRequest) (*row_source.ResolvedFromTime, *schema.TableSchema, error) {
	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	slog.Info("Collect - core plugin")

	// we expect the request to contain a custom table name, as this plugin only provides custom tables
	// validate there is a table and that is has a format
	if err := p.validateRequest(req); err != nil {
		return nil, nil, err
	}

	// now register a custom table in the factory using the options pattern
	table.RegisterCustomTable[*log.CustomLogTable](table.WithName(req.CustomTableSchema.Name))

	// now call the base implementation of Collect
	return p.PluginImpl.Collect(ctx, req)
}

// validate there is a table and that is has a format
func (p *Plugin) validateRequest(req *proto.CollectRequest) error {
	if req.CustomTableSchema == nil {
		return fmt.Errorf("custom table is required")
	}
	if req.CustomTableSchema.Name == "" {
		return fmt.Errorf("custom table name is required")
	}
	if req.SourceFormat == nil {
		return fmt.Errorf("source format is required")
	}
	return nil
}
