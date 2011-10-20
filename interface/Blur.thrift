namespace java com.nearinfinity.blur.thrift.generated
namespace rb blur
namespace perl Blur

/** 
  * BlurException that carries a message plus the original stack 
  * trace (if any). 
  */
exception BlurException {
  /** 
   * The message in the exception. 
   */
  1:string message,

  /** 
   * The original stack trace (if any). 
   */
  2:string stackTraceStr
}

/** 
  * The scoring type used during a SuperQuery to score multi Record 
  * hits within a ColumnFamily. 
  */
enum ScoreType {
  /** 
   * During a multi Record match, a calculation of the best match 
   * Record plus how often it occurs within the match Row produces 
   * the score that is used in the scoring of the SuperQuery. 
   */
  SUPER,

  /** 
   * During a multi Record match, the aggregate score of all the 
   * Records within a ColumnFamily is used in the scoring of the 
   * SuperQuery. 
   */
  AGGREGATE,

  /** 
   * During a multi Record match, the best score of all the 
   * Records within a ColumnFamily is used in the scoring of the 
   * SuperQuery. 
   */
  BEST,

  /** 
   * A constant score of 1 is used in the scoring of the SuperQuery.
   */
  CONSTANT
}

/**
  * The state of a query.
  */
enum QueryState {
  /** 
   * Query is running.
   */
  RUNNING,

  /** 
   * Query has been interrupted.
   */
  INTERRUPTED,

  /**
   * Query is complete.
   */
  COMPLETE
}

/**
 * Specifies the type of Row mutation that should occur during 
 * a mutation of a given Row.
 */
enum RowMutationType {
  /** 
   * Indicates that the entire Row is to be deleted. 
   */
  DELETE_ROW,

  /** 
   * Indicates that the entire Row is to be deleted, and then a new 
   * Row with the same id is to be added. 
   */
  REPLACE_ROW,

  /** 
   * Indicates that mutations of the underlying Records will be 
   * processed individually. 
   */
  UPDATE_ROW
}

/**
 * Specifies the type of Record mutation that should occur during 
 * a mutation of a given Record.
 */
enum RecordMutationType {
  /** 
   * Indicates the Record with the given recordId in the given Row 
   * is to be deleted. 
   */
  DELETE_ENTIRE_RECORD,

  /** 
   * Indicates the Record with the given recordId in the given Row 
   * is to be deleted, and a new Record with the same id is to be added. 
   */
  REPLACE_ENTIRE_RECORD,

  /**
   * Replace the columns that are specified in the Record mutation.
   */
  REPLACE_COLUMNS,

  /**
   * Append the columns in the Record mutation to the Record that 
   * could already exist.
   */
  APPEND_COLUMN_VALUES
}





/**
 * Column is the lowest storage element in Blur, it stores a single name and value pair.
 */
struct Column {
  /**
   * The name of the column.
   */
  1:string name,

  /**
   * The value to be indexed and stored.
   */
  2:string value
}

/**
 * Records contain a list of columns, multiple columns with the same name are allowed.
 */
struct Record {
  /**
   * Record id uniquely identifies a record within a single row.
   */
  1:string recordId,

  /**
   * The family in which this record resides.
   */
  2:string family,

  /**
   * A list of columns, multiple columns with the same name are allowed.
   */
  3:list<Column> columns
}

/**
 * Rows contain a list of records.
 */
struct Row {
  /**
   * The row id.
   */
  1:string id,

  /**
   * The list records within the row.  If paging is used this list will only 
   * reflect the paged records from the selector.
   */
  2:list<Record> records,

  /**
   * The total record count for the row.  If paging is used in a selector to page 
   * through records of a row, this count will reflect the entire row.
   */
  3:i32 recordCount
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
  2:binary filter
}

struct Facet {
  1:string queryStr,
  2:i64 minimumNumberOfBlurResults = 9223372036854775807
}

struct BlurQuery {
  1:SimpleQuery simpleQuery,
  2:ExpertQuery expertQuery,
  3:list<Facet> facets,
  4:Selector selector,
  5:bool allowStaleData = 0,
  6:bool useCacheIfPresent = 1,
  7:i64 start = 0,
  8:i32 fetch = 10, 
  9:i64 minimumNumberOfResults = 9223372036854775807,
  10:i64 maxQueryTime = 9223372036854775807,
  11:i64 uuid,
  12:string userContext,
  13:bool cacheResult = 1,
  14:i64 startTime = 0,
  15:bool modifyFileCaches = 1
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
  4:list<i64> facetCounts,
  5:list<BlurException> exceptions,
  6:BlurQuery query
}



struct RecordMutation {
  1:RecordMutationType recordMutationType,
  2:Record record
}

struct RowMutation {
  1:string table,
  2:string rowId,
  3:bool wal = 1,
  4:RowMutationType rowMutationType,
  5:list<RecordMutation> recordMutations
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

struct TableStats {
  1:string tableName,
  2:i64 bytes,
  3:i64 recordCount,
  4:i64 rowCount,
  5:i64 queries
}

struct Schema {
  1:string table,
  2:map<string,set<string>> columnFamilies
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

struct TableDescriptor {
  1:bool isEnabled = 1,
  2:AnalyzerDefinition analyzerDefinition,
  3:i32 shardCount = 1,
  4:string tableUri,
  5:string compressionClass = 'org.apache.hadoop.io.compress.DefaultCodec',
  6:i32 compressionBlockSize = 32768,
  7:string cluster
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


