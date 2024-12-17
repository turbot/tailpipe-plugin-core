package tables

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type LogTableConfig struct {
	// Optional: Custom log format
	LogFormat string `json:"log_format,omitempty" hcl:"log_format"`

	// Optional: provide the schema
	//Schema *table.RowSchemaConfig `hcl:"schema,block"`

	// Mappings to common fields
	// At least TPTimestamp must be specified in the mappings
	Mappings enrichment.CommonFieldsMappings `hcl:"mappings,block"`
}

// TODO K identifier?
func (c *LogTableConfig) Identifier() string {
	return "custom_log"
}

func (c *LogTableConfig) Validate() error {
	// if schema is provided, validate
	//if c.Schema != nil {
	//	// mode must not be dynamic (as that means NO columns are specified
	//	if c.Schema.Mode == schema.ModeDynamic && len(c.Schema.Columns) > 0 {
	//		return fmt.Errorf("schema.mode cannot be dynamic when columns are provided")
	//	}
	//	for _, col := range c.Schema.Columns {
	//		if col.Type == "" {
	//			return fmt.Errorf("column type must be provided")
	//		}
	//		if col.Name == "" {
	//			return fmt.Errorf("column name must be provided")
	//		}
	//		// validate name and type
	//		if !schema.IsValidColumnName(col.Name) {
	//			return fmt.Errorf("invalid column name %s", col.Name)
	//		}
	//		if !schema.IsValidColumnType(col.Type) {
	//			return fmt.Errorf("invalid column type %s", col.Type)
	//		}
	//	}
	//}

	// log format must be provided
	if len(c.LogFormat) == 0 {
		return fmt.Errorf("log_format must be provided")
	}
	// there must be a mapping for at least TpTimestamp
	if c.Mappings.TpTimestamp == "" {
		return fmt.Errorf("mappings must include a mapping for TpTimestamp")
	}
	return nil
}

// GetSchema implements DynamicTableConfig interface
func (c *LogTableConfig) GetSchema() *schema.RowSchema {
	//if c.Schema == nil {
	//	return nil
	//}
	//
	//return c.Schema.ToRowSchema()
	// TODO K
	return nil

}
