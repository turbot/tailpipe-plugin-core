package custom

//
//type CustomTableFactory struct {
//
//}
//
//func newCustomTableFactory() table.TableFactory {
//
//	return &CustomTableFactory{}
//}
//
//func (c CustomTableFactory) RegisterCollector(collectorFunc func() table.Collector) {
//	// will not be called
//}
//
//
//func (c CustomTableFactory) Initialized() bool {
//	return true
//}
//
//func (c CustomTableFactory) Init() error {
//	return nil
//}
//
//func (c CustomTableFactory) GetCollector(request *types.CollectRequest) (table.Collector, error) {
//		// return either CustomArtifactConversiopnTable or CustomTable
//		// based on the request
//
//		return &table.CollectorImpl[R, S, T]{
//			table: utils.InstanceOf[T](),
//		}
//}
//
//func (c CustomTableFactory) GetSchema() (schema.SchemaMap, error) {
//	// no schema to return
//	return nil, nil
//}
