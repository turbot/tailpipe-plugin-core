package log

import (
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/mappers"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type LogTable struct {
	table.TableWithFormatImpl[*formats.Custom]
	Name string
}

func (c *LogTable) GetSourceMetadata() ([]*table.SourceMetadata[*table.DynamicRow], error) {
	// c.Format will already be populated by our TableWithFormatImpl
	mapper, err := mappers.NewGrokMapper[*table.DynamicRow](c.Format.Layout, c.Format.Patterns)
	if err != nil {
		return nil, err
	}
	return []*table.SourceMetadata[*table.DynamicRow]{
		{
			// any artifact source
			SourceName: constants.ArtifactSourceIdentifier,
			Mapper:     mapper,
			Options: []row_source.RowSourceOption{
				artifact_source.WithRowPerLine(),
			},
		},
	}, nil
}

func (c *LogTable) Identifier() string {
	return c.Name
}

func (c *LogTable) EnrichRow(row *table.DynamicRow, sourceEnrichmentFields schema.SourceEnrichment) (*table.DynamicRow, error) {
	// tell the row to enrich itself using any mappings specified in the source format
	row.Enrich(sourceEnrichmentFields.CommonFields)
	return row, nil
}
