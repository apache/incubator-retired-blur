namespace java com.nearinfinity.blur.thrift.generated
namespace rb blur

exception BlurException {
  1:string message,
  2:string stackTraceStr
}

struct AlternateColumnDefinition {
  1:string analyzerClassName
}

struct ColumnDefinition {
  1:string analyzerClassName,
  2:bool fullTextIndex,
  3:map<string,AlternateColumnDefinition> alternateColumnDefinitions
}

struct ColumnFamilyDefinition {
  1:ColumnDefinition defaultDefinition,
  2:map<string,ColumnDefinition> columnDefinitions
}

struct AnalyzerDefinition {
  1:ColumnDefinition defaultDefinition,
  2:string fullTextAnalyzerClassName,
  3:map<string,ColumnFamilyDefinition> columnFamilyDefinitions
}

enum ScoreType {
  SUPER,
  AGGREGATE,
  BEST,
  CONSTANT
}

struct Selector {
  1:bool recordOnly,
  2:string locationId,
  3:string rowId,
  4:string recordId,
  5:set<string> columnFamiliesToFetch,
  6:map<string,set<string>> columnsToFetch,
  7:bool allowStaleData
}

struct Facet {
  1:string queryStr,
  2:i64 minimumNumberOfBlurResults
}

struct Column {
  1:string name,
  2:string value
}

struct Record {
  1:string recordId,
  2:string family,
  3:list<Column> columns
}

struct Row {
  1:string id,
  2:list<Record> records
}

struct FetchRowResult {
  1:Row row
}

struct FetchRecordResult {
  1:string rowid,
  2:Record record
}

struct FetchResult {
  1:bool exists,
  2:bool deleted,
  3:string table,
  4:FetchRowResult rowResult,
  5:FetchRecordResult recordResult
}

struct SimpleQuery {
  1:string queryStr,
  2:bool superQueryOn = 1,
  3:ScoreType type = ScoreType.SUPER, 
  4:string postSuperFilter,
  5:string preSuperFilter,
}

struct ExpertQuery {
  1:binary query,
  2:binary filter,
  3:binary sort
}

struct BlurQuery {
  1:SimpleQuery simpleQuery,
  2:ExpertQuery expertQuery,
  3:i64 start = 0,
  4:i32 fetch = 10, 
  5:i64 minimumNumberOfResults = 9223372036854775807,
  6:i64 maxQueryTime = 9223372036854775807,
  7:i64 uuid,
  8:string userId,
  9:bool resolveIds,
  10:list<Facet> facets,
  11:Selector selector,
  12:i64 startTime,
  13:bool cacheOnly = 0,
  14:bool allowStaleData
}

struct BlurResult {
  1:string locationId,
  2:double score,
  3:FetchResult fetchResult
}

struct BlurResults {
  1:i64 totalResults = 0,
  2:map<string,i64> shardInfo,
  3:list<BlurResult> results,
  4:list<BlurException> exceptions,
  5:BlurQuery query,
  6:i64 realTime,
  7:i64 cpuTime,
  8:list<i64> facetCounts
}

struct TableDescriptor {
  1:bool isEnabled,
  2:AnalyzerDefinition analyzerDefinition,
  3:i32 shardCount,
  4:string tableUri,
  5:string compressionClass,
  6:i32 compressionBlockSize,
  7:string cluster
}

enum QueryState {
  RUNNING,
  INTERRUPTED,
  COMPLETE
}

struct CpuTime {
  1:i64 cpuTime,
  2:i64 realTime
}

struct BlurQueryStatus {
  1:BlurQuery query,
  2:map<string,CpuTime> cpuTimes,
  3:i32 completeShards,
  4:i32 totalShards,
  5:QueryState state,
  6:i64 uuid
}

struct Schema {
  1:string table,
  2:map<string,set<string>> columnFamilies
}

enum RecordMutationType {
  DELETE_ENTIRE_RECORD,
  REPLACE_ENTIRE_RECORD,
  REPLACE_COLUMNS,
  APPEND_COLUMN_VALUES
}

struct RecordMutation {
  1:RecordMutationType recordMutationType,
  2:Record record
}

enum RowMutationType {
  DELETE_ROW,
  REPLACE_ROW,
  UPDATE_ROW
}

struct RowMutation {
  1:string table,
  2:string rowId,
  3:bool wal,
  4:RowMutationType rowMutationType,
  5:list<RecordMutation> recordMutations
}

struct TableStats {
  1:string tableName,
  2:i64 bytes,
  3:i64 recordCount,
  4:i64 rowCount,
  5:i64 queries
}

service Blur {

  list<string> shardClusterList() throws (1:BlurException ex)
  list<string> shardServerList(1:string cluster) throws (1:BlurException ex)
  list<string> controllerServerList() throws (1:BlurException ex)
  map<string,string> shardServerLayout(1:string table) throws (1:BlurException ex)

  list<string> tableList() throws (1:BlurException ex)
  TableDescriptor describe(1:string table) throws (1:BlurException ex)

  BlurResults query(1:string table, 2:BlurQuery blurQuery) throws (1:BlurException ex)
  void cancelQuery(1:string table, 2:i64 uuid) throws (1:BlurException ex)
  list<BlurQueryStatus> currentQueries(1:string table) throws (1:BlurException ex)

  Schema schema(1:string table) throws (1:BlurException ex)
  TableStats getTableStats(1:string table) throws (1:BlurException ex)
  list<string> terms(1:string table, 2:string columnFamily, 3:string columnName, 4:string startWith, 5:i16 size) throws (1:BlurException ex)
  i64 recordFrequency(1:string table, 2:string columnFamily, 3:string columnName, 4:string value) throws (1:BlurException ex)

  FetchResult fetchRow(1:string table, 2:Selector selector) throws (1:BlurException ex)

  void mutate(1:RowMutation mutation) throws (1:BlurException ex)
  void mutateBatch(1:list<RowMutation> mutations) throws (1:BlurException ex)

  void createTable(1:string table, 2:TableDescriptor tableDescriptor) throws (1:BlurException ex)
  void enableTable(1:string table) throws (1:BlurException ex)
  void disableTable(1:string table) throws (1:BlurException ex)
  void removeTable(1:string table, 2:bool deleteIndexFiles) throws (1:BlurException ex)
  
}


