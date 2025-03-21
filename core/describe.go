package core

import (
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// DescribeSources returns a map of source metadata - it is called by the CLI to determine which sources are
// provided by the core plugin
func DescribeSources() (types.SourceMetadataMap, error) {
	return row_source.Factory.DescribeSources()
}
