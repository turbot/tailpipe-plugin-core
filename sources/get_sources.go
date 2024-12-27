package sources

import "github.com/turbot/tailpipe-plugin-sdk/row_source"

func DescribeSources() row_source.SourceMetadataMap {
	return row_source.Factory.DescribeSources()
}
