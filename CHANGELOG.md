## v0.2.10 [2025-07-24]
* Recompiled plugin with [tailpipe-plugin-sdk v0.9.2](https://github.com/turbot/tailpipe-plugin-sdk/blob/develop/CHANGELOG.md#v092-2025-07-24) that fixes incorrect collection boundaries for zero-granularity data and a crash caused by null trunk states in the collection state. Also includes a security upgrade to the `oauth2` dependency.

## v0.2.9 [2025-07-02]
* Recompiled plugin with [tailpipe-plugin-sdk v0.9.1](https://github.com/turbot/tailpipe-plugin-sdk/blob/develop/CHANGELOG.md#v071-2025-07-02) 

## v0.2.8 [2025-07-02]
* Recompiled plugin with [tailpipe-plugin-sdk v0.9.0](https://github.com/turbot/tailpipe-plugin-sdk/blob/develop/CHANGELOG.md#v071-2025-07-02) 

## v0.2.7 [2025-06-05]
* Recompiled plugin with [tailpipe-plugin-sdk v0.7.2](https://github.com/turbot/tailpipe-plugin-sdk/blob/develop/CHANGELOG.md#v072-2025-06-04) that fixes an issue where the end time was not correctly recorded for collections using artifact sources. ([#38](https://github.com/turbot/tailpipe-plugin-core/pull/38))

## v0.2.6 [2025-06-04]
* Recompiled plugin with [tailpipe-plugin-sdk v0.7.1](https://github.com/turbot/tailpipe-plugin-sdk/blob/develop/CHANGELOG.md#v071-2025-06-04) that fixes an issue affecting collections using a file source. ([#36](https://github.com/turbot/tailpipe-plugin-core/pull/36))

## v0.2.5 [2025-05-19]
* Update tailpipe-plugin-sdk to v0.6.1.
  * Update checkJsonlSize to skip check if no min size is set. ([#204](https://github.com/turbot/tailpipe-plugin-sdk/issues/204))

## v0.2.4 [2025-05-16]
* Update tailpipe-plugin-sdk to v0.6.0
  * Add support for zip artifact loaders. ([#195](https://github.com/turbot/tailpipe-plugin-sdk/issues/195))
* Update pipe-fittings to v2.5.1
  * Add `remain` to `HclTag`

## v0.2.3 [2025-04-25]

* Update tailpipe-plugin-sdk to v0.5.0-rc.2
  * When describing a source, include all properties. ([#199](https://github.com/turbot/tailpipe-plugin-sdk/issues/199))
  * Add support for enforcing size limits on temporary directory `max_temp_dir_mb` by limiting total JSONL disk usage. ([#192](https://github.com/turbot/tailpipe-plugin-sdk/issues/192))
* Update pipe-fittings to v2.4.0-rc.5
   * Add `plugin_memory_max_mb`, `memory_max_mb`, `temp_dir_max_mb` options to TailpipeWorkspaceProfile. Also add `TEMP_DIR_MAX_MB` env var and `temp-dir-max-mb` argument. ([#707](https://github.com/turbot/pipe-fittings/issues/707))

## v0.2.2 [2025-04-25]

* Update tailpipe-plugin-sdk to v0.4.0
    * Add WithHeaderRowNotification RowSourceOption, which can be set to enable a mapper to be notified of the header row of an artifact. ([#186](https://github.com/turbot/tailpipe-plugin-sdk/issues/186))
    * Fix source file error for custom tables when using S3 or other external source. ([#188](https://github.com/turbot/tailpipe-plugin-sdk/issues/188))

## v0.2.1 [2025-04-16]

* Update pipe-fittings to v2.3.3    
  * Update auto_escape deprecation warning
  * Fix multiple backtick escaping.
  * Add comma separators to numeric output in query results.  ([#685](https://github.com/turbot/pipe-fittings/issues/685))
  * Add support to `querydisplay.ColumnValueAsString` for UUID/Decimal in format received from DuckDB.
* Update tailpipe-plugin-sdk to v0.3.1
  * Fix Column level `null_if` not being respected. ([#182](https://github.com/turbot/tailpipe-plugin-sdk/issues/182))
  * Fix missing required column is not being reported as a row error.  ([#181](https://github.com/turbot/tailpipe-plugin-sdk/issues/181))

## v0.2.0 [2025-15-04]
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
