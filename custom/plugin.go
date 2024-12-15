package custom

import (
	"context"
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-custom/tables"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"log"
	"log/slog"

	// reference the table package to ensure that the tables are registered by the init functions
	_ "github.com/turbot/tailpipe-plugin-custom/tables"
)

const PluginName = "dynamic"

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
func (p *Plugin) Init(context.Context) error {
	return nil
}

// Collect overrides the Collect method in PluginImpl - so we do not use the factory to create a collector,
// instead we create our own
func (p *Plugin) Collect(ctx context.Context, req *proto.CollectRequest) (*schema.RowSchema, error) {
	log.Println("[INFO] Collect")

	// map req to our internal type
	collectRequest, err := types.CollectRequestFromProto(req)
	if err != nil {
		slog.Error("CollectRequestFromProto failed", "error", err)

		return nil, err
	}
	tableName := collectRequest.PartitionData.Table

	var collector table.Collector
	switch req.SourceFormatData.Target {
	case constants.SourceFormatCustom:
		collector = &table.CollectorImpl[*table.DynamicRow, *tables.LogTableConfig]{
			Table: &tables.LogTable{
				Name: tableName,
			},
		}
	case constants.SourceFormatDelimited:
		collector = &table.ArtifactConversionCollector[*tables.DelimitedTableConfig]{}
	case constants.SourceFormatJsonLines:
	case constants.SourceFormatJson:
	default:

	}

	// initialise the collector
	if err := collector.Init(ctx, collectRequest); err != nil {
		return nil, err
	}

	// add ourselves as an observer
	if err := collector.AddObserver(p); err != nil {
		slog.Error("add observer error", "error", err)
		return nil, err
	}

	// create context containing execution id
	ctx = context_values.WithExecutionId(ctx, req.ExecutionId)

	// signal we have started
	if err := p.OnStarted(ctx, req.ExecutionId); err != nil {
		err := fmt.Errorf("error signalling started: %w", err)
		_ = p.OnCompleted(ctx, req.ExecutionId, 0, 0, nil, err)
	}

	go func() {
		// tell the collection to start collecting - this is a blocking call
		rowCount, chunksWritten, err := collector.Collect(ctx)
		timing := collector.GetTiming()
		// signal we have completed - pass error if there was one
		_ = p.OnCompleted(ctx, req.ExecutionId, rowCount, chunksWritten, timing, err)
	}()

	// return the schema (if available - this may be nil for dynamic tables, in which case the CLI will infer the schema)
	return collector.GetSchema()
}
