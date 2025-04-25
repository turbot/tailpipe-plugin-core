## v0.2.1 [2025-02-16]

* Update pipe-fittings to v2.3.3    
  * Update auto_escape deprecation warning
  * Fix multiple backtick escaping.
  * Add comma separators to numeric output in query results.  ([#685](https://github.com/turbot/pipe-fittings/issues/685))
  * Add support to `querydisplay.ColumnValueAsString` for UUID/Decimal in format received from DuckDB.
* Update tailpipe-plugin-sdk to v0.3.1
  * Fix Column level `null_if` not being respected. ([#182](https://github.com/turbot/tailpipe-plugin-sdk/issues/182))
  * Fix missing required column is not being reported as a row error.  ([#181](https://github.com/turbot/tailpipe-plugin-sdk/issues/181))

## v0.2.0 [2025-01-30]
* Add support for custom tables
* add support form custom formats
* export `DefaultDelimited` and `DefaultJsonLines` format presets.
* Add metadata to run plugin in-process to print metadata. 
* Update file source to allow for non-fatal errors. Closes #12 04/04/2025, 19:39

 
### v0.1.2 [2025-01-30]
* re-add DescribeSources

### v0.1.1 [2025-01-30]
* Update sdk to v0.1.0
* update pipe-fittings to v2.0.0

## v0.1.0 [2025-01-30]

Initial plugin release.

_What's new?_

- `file` source  
