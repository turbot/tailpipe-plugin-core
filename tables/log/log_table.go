package log

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/types"

	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

// CustomLogTable is a CustomTable implementation for a fully custom table,
// where the format and table def are provided by the partition config
type CustomLogTable struct {
	table.CustomTableImpl
}

func (c *CustomLogTable) Identifier() string {
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

func (c *CustomLogTable) GetSupportedFormats() *formats.SupportedFormats {
	return &formats.SupportedFormats{
		Formats: map[string]func() formats.Format{
			constants.SourceFormatGrok:  formats.NewGrok,
			constants.SourceFormatRegex: formats.NewRegex,
		},
	}
}

func (c *CustomLogTable) GetTableDefinition() *schema.TableSchema {
	// the log table has no fixed definition - it is defined purely in config
	return nil
}
