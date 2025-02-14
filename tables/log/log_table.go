package log

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type LogTable struct {
	table.CustomTableImpl[*table.DynamicRow]
	Name string
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

func (c *LogTable) Identifier() string {
	return c.Name
}
