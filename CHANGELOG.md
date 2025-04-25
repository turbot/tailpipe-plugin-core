## v0.1.4 [2025-01-30]

Add metadata printing. 
Replace GetSupportedFormats with GetDefaultFormat
Update file source to allow for non-fatal errors. Closes #12 04/04/2025, 19:39
export DefaultDelimited and DefaultJsonLines format presets.

Add support for custom formats (#8)

* Add GetSupportedFormat
* Remove schema from mappers
* 
Add support for custom tables (#6)

* Update to use go-kit v1.0.0
* GetSourceMetadata returns error
* formats moved to sdk
* Return error from EnrichRow
* Add support for predefined custom tables
* Add Required to ColumnSchema, respect this in RowSchema.MapRow
* remove proto.Table and CustomTableDef - instead add name to TableSchema (renamed fromRowSchema) and just use that
* Split RegisterCustomTable into RegisterCustomTable and RegisterPredefinedCustomTable
* 
### v0.1.2 [2025-01-30]
* re-add DescribeSources

### v0.1.1 [2025-01-30]
* Update sdk to v0.1.0
* update pipe-fittings to v2.0.0

## v0.1.0 [2025-01-30]

Initial plugin release.

_What's new?_

- `file` source  
