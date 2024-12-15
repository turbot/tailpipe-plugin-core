package tables

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type LogTable struct {
	Name string
}

func (c *LogTable) GetSourceMetadata(config *LogTableConfig) []*table.SourceMetadata[*table.DynamicRow] {
	return []*table.SourceMetadata[*table.DynamicRow]{
		{
			// any artifact source
			SourceName: constants.ArtifactSourceIdentifier,
			Mapper:     table.NewRowPatternMapper[*table.DynamicRow](config.LogFormat),
			Options: []row_source.RowSourceOption{
				artifact_source.WithRowPerLine(),
			},
		},
	}
}

func (c *LogTable) Identifier() string {
	return c.Name
}

func (c *LogTable) EnrichRow(row *table.DynamicRow, config *LogTableConfig, sourceEnrichmentFields enrichment.SourceEnrichment) (*table.DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the config
	row.Enrich(config.Mappings, sourceEnrichmentFields.CommonFields)
	return row, nil
}
