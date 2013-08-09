/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace java org.apache.blur.thrift.generated
namespace rb blur
namespace perl Blur

enum ErrorType {
  UNKNOWN,
  QUERY_CANCEL,
  QUERY_TIMEOUT,
  BACK_PRESSURE
}

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
  2:string stackTraceStr,

  3:ErrorType errorType
}

/** 
  * The scoring type used during a SuperQuery to score multi Record hits within a ColumnFamily.<br/><br/>
  * SUPER - During a multi Record match, a calculation of the best match Record plus how often it occurs within the match Row produces the score that is used in the scoring of the SuperQuery.<br/><br/>
  * AGGREGATE - During a multi Record match, the aggregate score of all the Records within a ColumnFamily is used in the scoring of the SuperQuery.<br/><br/>
  * BEST - During a multi Record match, the best score of all the Records within a ColumnFamily is used in the scoring of the SuperQuery.<br/><br/>
  * CONSTANT - A constant score of 1 is used in the scoring of the SuperQuery.<br/>
  */
enum ScoreType {
  SUPER,
  AGGREGATE,
  BEST,
  CONSTANT
}

/**
  * The state of a query.<br/><br/>
  * RUNNING - Query is running.<br/><br/>
  * INTERRUPTED - Query has been interrupted.<br/><br/>
  * COMPLETE - Query is complete.<br/>
  */
enum QueryState {
  RUNNING,
  INTERRUPTED,
  COMPLETE,
  BACK_PRESSURE_INTERRUPTED
}

/**
  * NOT_FOUND : when UUID is not found<br/><br/>
  * FOUND : when UUID is present<br/>
  */
enum Status {
    NOT_FOUND,
    FOUND
}

/**
 * Specifies the type of Row mutation that should occur during a mutation of a given Row.<br/><br/>
 * DELETE_ROW - Indicates that the entire Row is to be deleted.  No changes are made if the specified row does not exist.<br/><br/>
 * REPLACE_ROW - Indicates that the entire Row is to be deleted, and then a new Row with the same id is to be added.  If the specified row does not exist, the new row will still be created.<br/><br/>
 * UPDATE_ROW - Indicates that mutations of the underlying Records will be processed individually.  Mutation will result in a BlurException if the specified row does not exist.<br/>
 */
enum RowMutationType {
  DELETE_ROW,
  REPLACE_ROW,
  UPDATE_ROW
}

/**
 * Specifies the type of Record mutation that should occur during a mutation of a given Record.<br/><br/>
 * DELETE_ENTIRE_RECORD -  Indicates the Record with the given recordId in the given Row is to be deleted.  If the target record does not exist, then no changes are made.<br/><br/>
 * REPLACE_ENTIRE_RECORD - Indicates the Record with the given recordId in the given Row is to be deleted, and a new Record with the same id is to be added. If the specified record does not exist the new record is still added.<br/><br/>
 * REPLACE_COLUMNS - Replace the columns that are specified in the Record mutation.  If the target record does not exist then this mutation will result in a BlurException.<br/><br/>
 * APPEND_COLUMN_VALUES - Append the columns in the Record mutation to the Record that could already exist.  If the target record does not exist then this mutation will result in a BlurException.<br/>
 */
enum RecordMutationType {
  DELETE_ENTIRE_RECORD,
  REPLACE_ENTIRE_RECORD,
  REPLACE_COLUMNS,
  APPEND_COLUMN_VALUES
}

/**
 * See shardServerLayoutOptions method in the Blur service for details.
 */
enum ShardState {
  OPENING,
  OPEN,
  OPENING_ERROR,
  CLOSING,
  CLOSED,
  CLOSING_ERROR
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

/**
 * The SimpleQuery object holds the query string (normal Lucene syntax), filters and type of scoring (used when super query is on).
 */
struct SimpleQuery {
  /**
   * A Lucene syntax based query.
   */
  1:string queryStr,
  /**
   * If the super query is on, meaning the query will be perform against all the records (joining records in some cases) and the result will be Rows (groupings of Record).
   */
  2:bool superQueryOn = 1,
  /**
   * The scoring type, see the document on ScoreType for explanation of each score type.
   */
  3:ScoreType type = ScoreType.SUPER, 
  /**
   * The post super filter (normal Lucene syntax), is a filter performed after the join to filter out entire rows from the results.
   */
  4:string postSuperFilter,
  /**
   * The pre super filter (normal Lucene syntax), is a filter performed before the join to filter out records from the results.
   */
  5:string preSuperFilter
}

/**
 * The HighlightOptions controls how the data is fetched and returned.
 */
struct HighlightOptions {
  /**
   * The original query is required if used in the Blur.fetchRow call.  If 
   * the highlightOptions is used in a call to Blur.query then the SimpleQuery 
   * passed into the call via the BlurQuery will be used if this simpleQuery is 
   * null.  So that means if you use highlighting from the query call you can 
   * leave this attribute null and it will default to the normal behavior.
   */
  1:SimpleQuery simpleQuery,

  /**
   * The pre tag is the tag that marks the beginning of the highlighting.
   */
  2:string preTag = "<<<",

  /**
   * The post tag is the tag that marks the end of the highlighting.
   */
  3:string postTag = ">>>"
}

/**
 * Select carries the request for information to be retrieved from the stored columns.
 */
struct Selector {
  /**
   * Fetch the Record only, not the entire Row.
   */
  1:bool recordOnly,
  /**
   * WARNING: This is an internal only attribute and is not intended for use by clients.
   * The location id of the Record or Row to be fetched.
   */
  2:string locationId,
  /**
   * The row id of the Row to be fetched, not to be used with location id.
   */
  3:string rowId,
  /**
   * The record id of the Record to be fetched, not to be used with location id.  However the row id needs to be provided to locate the correct Row with the requested Record.
   */
  4:string recordId,
  /**
   * The column families to fetch.  If null, fetch all.  If empty, fetch none.
   */
  5:set<string> columnFamiliesToFetch,
  /**
   * The columns in the families to fetch.  If null, fetch all.  If empty, fetch none.
   */
  6:map<string,set<string>> columnsToFetch,
  /**
   * @deprecated This value is no longer used.  This allows the fetch to see the most current data that has been added to the table.
   */
  7:bool allowStaleData,
  /**
   * Only valid for Row fetches, the record in the row to start fetching.  If the row contains 1000 
   * records and you want the first 100, then this value is 0.  If you want records 300-400 then this 
   * value would be 300.  If startRecord is beyond the end of the row, the row will be null in the 
   * FetchResult.  Used in conjunction with maxRecordsToFetch.
   */
  8:i32 startRecord = 0,
  /**
   * Only valid for Row fetches, the number of records to fetch.  If the row contains 1000 records 
   * and you want the first 100, then this value is 100.  If you want records 300-400 then this value 
   * would be 100.  Used in conjunction with maxRecordsToFetch. By default this will fetch the first 
   * 1000 records of the row.
   */
  9:i32 maxRecordsToFetch = 1000,
  /**
   * The HighlightOptions object controls how the data is highlighted.  If null no highlighting will occur.
   */
  10:HighlightOptions highlightOptions
}

/**
 * FetchRowResult contains row result from a fetch.
 */
struct FetchRowResult {
  /**
   * The row fetched.
   */
  1:Row row
}

/**
 * FetchRecordResult contains rowid of the record and the record result from a fetch.
 */
struct FetchRecordResult {
  /**
   * The row id of the record being fetched.
   */
  1:string rowid,
  /**
   * The record fetched.
   */
  2:Record record
}

/**
 * FetchResult contains the row or record fetch result based if the Selector was going to fetch the entire row or a single record.
 */
struct FetchResult {
  /**
   * True if the result exists, false if it doesn't.
   */
  1:bool exists,
  /**
   * If the row was marked as deleted.
   */
  2:bool deleted,
  /**
   * The table the fetch result came from.
   */
  3:string table,
  /**
   * The row result if a row was selected form the Selector.
   */
  4:FetchRowResult rowResult,
  /**
   * The record result if a record was selected form the Selector.
   */
  5:FetchRecordResult recordResult
}

/**
 * The expert query allows for submission of a serialized query and filter object to be executed against all the queries.
 */
struct ExpertQuery {
  /**
   * The serialized query.
   */
  1:binary query,
  /**
   * The serialized filter.
   */
  2:binary filter
}

/**
 * Blur facet.
 */
struct Facet {
  1:string queryStr,
  2:i64 minimumNumberOfBlurResults = 9223372036854775807
}

/**
 *
 */
struct BlurQuery {
  /**
   *
   */
  1:SimpleQuery simpleQuery,
  /**
   *
   */
  2:ExpertQuery expertQuery,
  /**
   *
   */
  3:list<Facet> facets,
  /**
   * Selector is used to fetch data in the search results, if null only location ids will be fetched.
   */
  4:Selector selector,
  /**
   * @deprecated This value is no longer used.  This allows the query to see the most current data that has been added to the table.
   */
  5:bool allowStaleData = 0,
  /**
   *
   */
  6:bool useCacheIfPresent = 1,
  /**
   *
   */
  7:i64 start = 0,
  /**
   *
   */
  8:i32 fetch = 10, 
  /**
   *
   */
  9:i64 minimumNumberOfResults = 9223372036854775807,
  /**
   *
   */
  10:i64 maxQueryTime = 9223372036854775807,
  /**
   *
   */
  11:i64 uuid,
  /**
   *
   */
  12:string userContext,
  /**
   *
   */
  13:bool cacheResult = 1,
  /**
   *
   */
  14:i64 startTime = 0,
  /**
   *
   */
  15:bool modifyFileCaches = 1
}

/**
 *
 */
struct BlurResult {
  /**
   * WARNING: This is an internal only attribute and is not intended for use by clients.
   */
  1:string locationId,
  /**
   *
   */
  2:double score,
  /**
   *
   */
  3:FetchResult fetchResult
}

/**
 *
 */
struct BlurResults {
  /**
   *
   */
  1:i64 totalResults = 0,
  /**
   *
   */
  2:map<string,i64> shardInfo,
  /**
   *
   */
  3:list<BlurResult> results,
  /**
   *
   */
  4:list<i64> facetCounts,
  /**
   *
   */
  5:list<BlurException> exceptions,
  /**
   *
   */
  6:BlurQuery query
}

/**
 *
 */
struct RecordMutation {
  /**
   *
   */
  1:RecordMutationType recordMutationType,
  /**
   *
   */
  2:Record record
}

/**
 *
 */
struct RowMutation {
  /**
   * The that that the row mutation is to act upon.
   */
  1:string table,
  /**
   * The row id that the row mutation is to act upon.
   */
  2:string rowId,
  /**
   * Write ahead log, by default all updates are written to a write ahead log before the update is applied.  That way if a failure occurs before the index is committed the WAL can be replayed to recover any data that could have been lost.
   */
  3:bool wal = 1,
  4:RowMutationType rowMutationType,
  5:list<RecordMutation> recordMutations,
  /**
   * On mutate waits for the mutation to be visible to queries and fetch requests.
   */
  6:bool waitToBeVisible = 0
}

/**
 * Holds the cpu time for a query executing on a single shard in a table.
 */
struct CpuTime {
  /**
   * The total cpu time for the query on the given shard.
   */
  1:i64 cpuTime,
  /**
   * The real time of the query execution for a given shard.
   */
  2:i64 realTime
}

/**
 * The BlurQueryStatus object hold the status of BlurQueries.  The state of the query
 * (QueryState), the number of shards the query is executing against, the number of 
 * shards that are complete, etc.
 */
struct BlurQueryStatus {
  /**
   * The original query.
   */
  1:BlurQuery query,
  /**
   * A map of shard names to CpuTime, one for each shard in the table.
   */
  2:map<string,CpuTime> cpuTimes,
  /**
   * The number of completed shards.  The shard server will respond with 
   * how many are complete on that server, while the controller will aggregate 
   * all the shard server completed totals together.
   */
  3:i32 completeShards,
  /**
   * The total number of shards that the query is executing against.  The shard 
   * server will respond with how many shards are being queried on that server, while 
   * the controller will aggregate all the shard server totals together.
   */
  4:i32 totalShards,
  /**
   * The state of the query.  e.g. RUNNING, INTERRUPTED, COMPLETE
   */
  5:QueryState state,
  /**
   * The uuid of the query.
   */
  6:i64 uuid
  /**
   * The status of the query NOT_FOUND if uuid is not found else FOUND
   */
  7:Status status
}

/**
 *
 */
struct TableStats {
  /**
   *
   */
  1:string tableName,
  /**
   *
   */
  2:i64 bytes,
  /**
   *
   */
  3:i64 recordCount,
  /**
   *
   */
  4:i64 rowCount,
  /**
   *
   */
  5:i64 queries
}

/**
 *
 */
struct Schema {
  /**
   *
   */
  1:string table,
  /**
   *
   */
  2:map<string,set<string>> columnFamilies
}

/**
 *
 */
struct AlternateColumnDefinition {
  /**
   *
   */
  1:string analyzerClassName
}

/**
 *
 */
struct ColumnDefinition {
  1:string analyzerClassName = "org.apache.blur.analysis.NoStopWordStandardAnalyzer",
  2:bool fullTextIndex,
  3:map<string,AlternateColumnDefinition> alternateColumnDefinitions
}

/**
 *
 */
struct ColumnFamilyDefinition {
  /**
   *
   */
  1:ColumnDefinition defaultDefinition,
  /**
   *
   */
  2:map<string,ColumnDefinition> columnDefinitions
}

/**
 *
 */
struct AnalyzerDefinition {
  /**
   *
   */
  1:ColumnDefinition defaultDefinition,
  /**
   *
   */
  2:string fullTextAnalyzerClassName = "org.apache.blur.analysis.NoStopWordStandardAnalyzer",
  /**
   *
   */
  3:map<string,ColumnFamilyDefinition> columnFamilyDefinitions
}

/**
 *
 */
struct ColumnPreCache {
  /**
   * This map sets what column families and columns to prefetch into block cache on shard open.
   */
  1:list<string> preCacheCols
}

/**
 *
 */
struct TableDescriptor {
  /**
   * Is the table enabled or not, enabled by default.
   */
  1:bool isEnabled = 1,
  /**
   * The number of shards within the given table.
   */
  3:i32 shardCount = 1,
  /**
   * The location where the table should be stored this can be "file:///" for a local instance of Blur or "hdfs://" for a distributed installation of Blur.
   */
  4:string tableUri,
  /**
   * The cluster where this table should be created.
   */
  7:string cluster = 'default',
  /**
   * The table name.
   */
  8:string name,
  /**
   * Sets the similarity class in Lucene.
   */
  9:string similarityClass,
  /**
   * Should block cache be enable or disabled for this table.
   */
  10:bool blockCaching = 1,
  /**
   * The files extensions that you would like to allow block cache to to cache.  If null (default) everything is cached.
   */
  11:set<string> blockCachingFileTypes,
  /**
   * If a table is set to be readonly, that means that mutates through Thrift are NOT allowed.  However 
   * updates through MapReduce are allowed and in fact they are only allowed if the table is in readOnly mode.
   */
  12:bool readOnly = 0,
  /**
   * Sets what column families and columns to prefetch into block cache on shard open.
   */
  13:ColumnPreCache columnPreCache,
  /**
   * The table properties that can modify the default behavior of the table.  TODO: Document all options.
   */
  14:map<string,string> tableProperties,
  /**
   * Whether strict types are enabled or not (default).  If they are enabled no column can be added without first having it's type defined.
   */
  15:bool strictTypes = false,
  /**
   * If strict is not enabled, the default field type.
   */
  16:string defaultMissingFieldType = "text",
  /**
   * If strict is not enabled, defines whether or not field less indexing is enabled on the newly created fields.
   */
  17:bool defaultMissingFieldLessIndexing = true,
  /**
   * If strict is not enabled, defines the properties to be used in the new field creation.
   */
  18:map<string,string> defaultMissingFieldProps
}

struct Metric {
  1:string name,
  2:map<string,string> strMap,
  3:map<string,i64> longMap,
  4:map<string,double> doubleMap
}

/**
 *
 */
service Blur {

  /**
   * Returns a list of all the shard clusters.
   */
  list<string> shardClusterList() throws (1:BlurException ex)
  /**
   * Returns a list of all the shard servers for the given cluster.
   * @param cluster the cluster name.
   */
  list<string> shardServerList(1:string cluster) throws (1:BlurException ex)
  /**
   * Returns a list of all the controller servers.
   */
  list<string> controllerServerList() throws (1:BlurException ex)
  /**
   * Returns a map of the layout of the given table, where the key is the shard name 
   * and the value is the shard server.<br><br>
   * This method will return the "correct" layout for the given shard, or the 
   * "correct" layout of cluster if called on a controller.<br><br>
   * The meaning of correct:<br>Given the current state of the shard cluster with failures taken 
   * into account, the correct layout is what the layout should be given the current state.  In
   * other words, what the shard server should be serving.  The act of calling the shard 
   * server layout method with the NORMAL option will block until the layout shard server 
   * matches the correct layout.  Meaning it will block until indexes that should be open are 
   * open and ready for queries.  However indexes are lazily closed, so if a table is being 
   * disabled then the call will return immediately with an empty map, but the indexes may
   * not be close yet.<br><br>
   * @param table the table name.
   */
  map<string,string> shardServerLayout(1:string table) throws (1:BlurException ex)

  /**
   * Returns a map of the layout of the given table, where the key is the shard name and the 
   * value is the shard server.<br><br>
   * This method will return immediately with what shards are currently 
   * open in the shard server.  So if a shard is being moved to another server and is being 
   * closed by this server it WILL be returned in the map.  The shardServerLayout method would not return 
   * the shard given the same situation.
   * @param table the table name.
   * @param layoutOptions the layout options.
   */
  map<string,map<string,ShardState>> shardServerLayoutState(1:string table) throws (1:BlurException ex)
  /**
   * Returns a list of the table names across all shard clusters.
   */
  list<string> tableList() throws (1:BlurException ex)
  /**
   * Returns a list of the table names for the given cluster.
   * @param cluster the cluster name.
   */
  list<string> tableListByCluster(1:string cluster) throws (1:BlurException ex)
  /**
   * Returns a table descriptor for the given table.
   * @param table the table name.
   */
  TableDescriptor describe(1:string table) throws (1:BlurException ex)

  /**
   * Executes a query against a the given table and returns the results.  If this method is executed against a controller the results will contain the aggregated results from all the shards.  If this method is executed against a shard server the results will only contain aggregated results from the shards of the given table that are being served on the shard server, if any.
   * @param table the table name.
   * @param blurQuery the query to execute.
   */
  BlurResults query(1:string table, 2:BlurQuery blurQuery) throws (1:BlurException ex)

  /**
   * Parses the given query and return the string represents the query.
   * @param table the table name.
   * @param simpleQuery the query to parse.
   */
  string parseQuery(1:string table, 2:SimpleQuery simpleQuery) throws (1:BlurException ex)

  /**
   * Cancels a query that is executing against the given table with the given uuid.  Note, the cancel call maybe take some time for the query actually stops executing.
   * @param table the table name.
   * @param uuid the uuid of the query.
   */
  void cancelQuery(1:string table, 2:i64 uuid) throws (1:BlurException ex)

  /**
   * @deprecated This method should avoided, @see #queryStatusIdList and #queryStatusById.
   * @param table the table name.
   */
  list<BlurQueryStatus> currentQueries(1:string table) throws (1:BlurException ex)

  /**
   * Returns a list of the query ids of queries that have recently been executed for the given table.
   * @param table the table name.
   */
  list<i64> queryStatusIdList(1:string table) throws (1:BlurException ex)
  /**
   * Returns the query status for the given table and query uuid.
   * @param table the table name.
   * @param uuid the uuid of the query.
   */
  BlurQueryStatus queryStatusById(1:string table, 2:i64 uuid) throws (1:BlurException ex)

  Schema schema(1:string table) throws (1:BlurException ex)
  TableStats getTableStats(1:string table) throws (1:BlurException ex)
  TableStats tableStats(1:string table) throws (1:BlurException ex)
  list<string> terms(1:string table, 2:string columnFamily, 3:string columnName, 4:string startWith, 5:i16 size) throws (1:BlurException ex)
  i64 recordFrequency(1:string table, 2:string columnFamily, 3:string columnName, 4:string value) throws (1:BlurException ex)

  FetchResult fetchRow(1:string table, 2:Selector selector) throws (1:BlurException ex)

  void mutate(1:RowMutation mutation) throws (1:BlurException ex)
  void mutateBatch(1:list<RowMutation> mutations) throws (1:BlurException ex)

  void createTable(1:TableDescriptor tableDescriptor) throws (1:BlurException ex)
  void enableTable(1:string table) throws (1:BlurException ex)
  void disableTable(1:string table) throws (1:BlurException ex)
  void removeTable(1:string table, 2:bool deleteIndexFiles) throws (1:BlurException ex)

  void optimize(1:string table, 2:i32 numberOfSegmentsPerShard) throws (1:BlurException ex)
  
  bool isInSafeMode(1:string cluster) throws (1:BlurException ex)

  map<string,string> configuration() throws (1:BlurException ex)

  map<string,Metric> metrics(1:set<string> metrics) throws (1:BlurException ex)

}


