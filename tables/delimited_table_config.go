package tables

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/enrichment"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type DelimitedTableConfig struct {
	// specify the delimiter character
	Separator *string `hcl:"separator"`
	// specify the comment character
	Comment *string `hcl:"comment"`
	// specify the header mode - supported values: "auto", "off", 	"on"
	Header table.CsvHeaderMode `hcl:"header,optional"`

	// TODO K MAPPINGS AND SCHEMA TO CHANGE
	// Mappings to common fields
	Mappings *enrichment.CommonFieldsMappings `hcl:"mappings,block"`

	// Optional: provide the schema
	Schema *table.RowSchemaConfig `hcl:"schema,block"`
}

func (c *DelimitedTableConfig) Validate() error {
	// default header to auto
	if c.Header == "" {
		c.Header = table.CsvHeaderModeAuto
	}

	return nil
}

func (c *DelimitedTableConfig) Identifier() string {
	return constants.SourceFormatDelimited
}

func (c *DelimitedTableConfig) GetSchema() *schema.RowSchema {
	if c.Schema == nil {
		return nil
	}

	return c.Schema.ToRowSchema()
}

// GetCsvOpts converts the DelimitedTableConfig to a slice of CsvToJsonOpts
func (c *DelimitedTableConfig) GetCsvOpts() []table.CsvToJsonOpts {
	var opts []table.CsvToJsonOpts

	if c.Separator != nil {
		opts = append(opts, table.WithCsvDelimiter(*c.Separator))
	}
	if c.Comment != nil {
		opts = append(opts, table.WithCsvComment(*c.Comment))
	}
	if c.Header != table.CsvHeaderModeAuto {
		opts = append(opts, table.WithCsvHeaderMode(c.Header))
	}
	if c.Schema != nil {
		opts = append(opts, table.WithCsvSchema(c.Schema.ToRowSchema()))
	}
	opts = append(opts, table.WithMappings(c.Mappings.AsMap()))
	return opts
}
