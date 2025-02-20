package log

import (
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/parse"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

// LogTable is a CustomTable implementation for a fully custom table,
// where the format and table def are provided by the partition config
type LogTable struct {
	table.CustomTableImpl
}

func (c *LogTable) Identifier() string {
	return c.Schema.Name
}

// GetFormat implements the CustomTable interface
// just return the Format field
// (for 'predefined' custom table types this will be implemented by returning the predefined format)
func (c *LogTable) GetFormat() parse.Config {
	return c.Format
}

// GetSchema implements the CustomTable interface
// just return the TableDef field
// (for 'predefined' custom table types this will be implemented by returning the predefined table def)
func (c *LogTable) GetSchema() *schema.TableSchema {
	return c.Schema
}

func (c *LogTable) GetSourceMetadata() ([]*table.SourceMetadata[*table.DynamicRow], error) {
	// ask our custom table for the mapper
	mapper, err := c.GetMapper()
	if err != nil {
		return nil, fmt.Errorf("error creating '%s' mapper for custom table '%s': %w", c.Format.Identifier(), c.Identifier(), err)
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
