package tables

//
//// init registers the table
//func init() {
//	// Register the table, with type parameters:
//	// 1. row struct
//	// 2. table config struct
//	// 3. table implementation
//	table.RegisterTable[*table.DynamicRow, *DelimitedTableConfig, *CsvTable]()
//}
//
//const CsvTableIdentifier = "dynamic_csv"
//
//type CsvTable struct {
//	table.ArtifactToJsonConverterImpl[*DelimitedTableConfig]
//}
//
//func (c *CsvTable) Identifier() string {
//	return CsvTableIdentifier
//}
//
////func (c *CsvTable) ArtifactToJSON(ctx context.Context, sourceFile , executionId string, chunkNumber int, config *DelimitedTableConfig) (int, int, error){
////	// convert our config into a slice of CsvToJsonOpts
////	opts := config.GetCsvOpts()
////
////	// call into the ArtifactToJsonConverterImpl to do the conversion
////	return c.CsvToJSONL(ctx, sourceFile, executionId, chunkNumber, opts)
////}
