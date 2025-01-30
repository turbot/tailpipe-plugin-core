package core

import "github.com/turbot/tailpipe-plugin-sdk/row_source"

func DescribeSources() (row_source.SourceMetadataMap, error) {
	return row_source.Factory.DescribeSources()
}
