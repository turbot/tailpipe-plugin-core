package log

import (
	"fmt"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// CustomLogTable is a CustomTable implementation for a fully custom table,
// where the format and table def are provided by the partition config
type CustomLogTable struct {
	table.CustomTableImpl
}

func (c *CustomLogTable) Identifier() string {
	// if the schema has not been set, return the default identifier
	if c.Schema == nil {
		return "custom_log_table"
	}
	return c.Schema.Name
}

func (c *CustomLogTable) GetSourceMetadata() ([]*table.SourceMetadata[*types.DynamicRow], error) {
	// ask our format for the mapper
	mapper, err := c.Format.GetMapper()
	if err != nil {
		return nil, fmt.Errorf("error creating '%s' mapper for custom table '%s': %w", c.Format.Identifier(), c.Identifier(), err)
	}

	return []*table.SourceMetadata[*types.DynamicRow]{
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

func (c *CustomLogTable) GetTableDefinition() *schema.TableSchema {
	// the log table has no fixed definition - it is defined purely in config
	return nil
}
