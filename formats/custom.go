package formats

import (
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type Custom struct {
	Pattern string `hcl:"pattern"`

	// the roq schema must at the minimum provide mapping for the tp_timestamp field
	Schema *schema.RowSchema `hcl:"schema,block"`
}

func (c *Custom) Validate() error {
	return nil
}

func (c *Custom) Identifier() string {
	return constants.SourceFormatCustom
}

func (c *Custom) GetSchema() *schema.RowSchema {
	//if c.Schema == nil {
	//	return nil
	//}
	//
	//return c.Schema.ToRowSchema()
	return nil
}
