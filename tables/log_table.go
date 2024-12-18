package tables

import (
	"github.com/turbot/tailpipe-plugin-custom/formats"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type LogTable struct {
	table.TableWithFormatImpl[*formats.Custom]
	Name string
}

func (c *LogTable) GetSourceMetadata() []*table.SourceMetadata[*table.DynamicRow] {
	return []*table.SourceMetadata[*table.DynamicRow]{
		{
			// any artifact source
			SourceName: constants.ArtifactSourceIdentifier,
			// format should have been set for us
			Mapper: mappers.NewGonxMapper[*table.DynamicRow](c.Format.Pattern),
			Options: []row_source.RowSourceOption{
				artifact_source.WithRowPerLine(),
			},
		},
	}
}

func (c *LogTable) Identifier() string {
	return c.Name
}

func (c *LogTable) EnrichRow(row *table.DynamicRow, sourceEnrichmentFields enrichment.SourceEnrichment) (*table.DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	row.Enrich(sourceEnrichmentFields.CommonFields)
	return row, nil
}
