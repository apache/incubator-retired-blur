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

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">The error type of a BlurException.</td></tr>
  * <tr><td>UNKNOWN</td><td>Unknown error.</td></tr>
  * <tr><td>QUERY_CANCEL</td><td>Query has been cancel.</td></tr>
  * <tr><td>QUERY_TIMEOUT</td><td>Query has timed out.</td></tr>
  * <tr><td>BACK_PRESSURE</td><td>Server has run out of memory and is trying to prevent a failure.</td></tr>
  * <tr><td>REQUEST_TIMEOUT</td><td>The TCP connection has timed out.</td></tr>
  * </table>
  */
enum ErrorType {
  UNKNOWN,
  QUERY_CANCEL,
  QUERY_TIMEOUT,
  BACK_PRESSURE,
  REQUEST_TIMEOUT
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
  * TimeoutException occurs before the network connection timeout 
  * happens so that the client can reconnect. 
  */
exception TimeoutException {
  1:i64 instanceExecutionId
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">The scoring type used during a SuperQuery to score multi Record hits within a ColumnFamily.</td></tr>
  * <tr><td>SUPER</td><td>During a multi Record match, a calculation of the best match Record plus how often it occurs within the match Row produces the score that is used in the scoring of the SuperQuery.</td></tr>
  * <tr><td>AGGREGATE</td><td>During a multi Record match, the aggregate score of all the Records within a ColumnFamily is used in the scoring of the SuperQuery.</td></tr>
  * <tr><td>BEST</td><td>During a multi Record match, the best score of all the Records within a ColumnFamily is used in the scoring of the SuperQuery.</td></tr>
  * <tr><td>CONSTANT</td><td>A constant score of 1 is used in the scoring of the SuperQuery.</td></tr>
  * </table>
  */
enum ScoreType {
  SUPER,
  AGGREGATE,
  BEST,
  CONSTANT
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">The state of a query.</td></tr>
  * <tr><td>RUNNING</td><td>Query is running.</td></tr>
  * <tr><td>INTERRUPTED</td><td>Query has been interrupted.</td></tr>
  * <tr><td>COMPLETE</td><td>Query is complete.</td></tr>
  * </table>
  */
enum QueryState {
  RUNNING,
  INTERRUPTED,
  COMPLETE,
  BACK_PRESSURE_INTERRUPTED
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td>NOT_FOUND</td><td>Blur status UUID is not found.</td></tr>
  * <tr><td>FOUND</td><td>Blur status UUID is present.</td></tr>
  * </table>
  */
enum Status {
  NOT_FOUND,
  FOUND
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">Specifies the type of Row mutation that should occur during a mutation of a given Row.</td></tr>
  * <tr><td>DELETE_ROW</td><td>Indicates that the entire Row is to be deleted.  No changes are made if the specified row does not exist.</td></tr>
  * <tr><td>REPLACE_ROW</td><td>Indicates that the entire Row is to be deleted, and then a new Row with the same id is to be added.  If the specified row does not exist, the new row will still be created.</td></tr>
  * <tr><td>UPDATE_ROW</td><td>Indicates that mutations of the underlying Records will be processed individually.  Mutation will result in a BlurException if the specified row does not exist.</td></tr>
  * </table>
  */
enum RowMutationType {
  DELETE_ROW,
  REPLACE_ROW,
  UPDATE_ROW
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">Specifies the type of Record mutation that should occur during a mutation of a given Record.</td></tr>
  * <tr><td>DELETE_ENTIRE_RECORD</td><td>Indicates the Record with the given recordId in the given Row is to be deleted.  If the target record does not exist, then no changes are made.</td></tr>
  * <tr><td>REPLACE_ENTIRE_RECORD</td><td>Indicates the Record with the given recordId in the given Row is to be deleted, and a new Record with the same id is to be added. If the specified record does not exist the new record is still added.</td></tr>
  * <tr><td>REPLACE_COLUMNS</td><td>Replace the columns that are specified in the Record mutation.  If the target record does not exist then this mutation will result in a BlurException.</td></tr>
  * <tr><td>APPEND_COLUMN_VALUES</td><td>Append the columns in the Record mutation to the Record that could already exist.  If the target record does not exist then this mutation will result in a BlurException.</td></tr>
  * </table>
  */
enum RecordMutationType {
  DELETE_ENTIRE_RECORD,
  REPLACE_ENTIRE_RECORD,
  REPLACE_COLUMNS,
  APPEND_COLUMN_VALUES
}

/**
  * <table class="table-bordered table-striped table-condensed">
  * <tr><td colspan="2">The shard state, see shardServerLayoutOptions method in the Blur service for details.</td></tr>
  * <tr><td>OPENING</td><td>The shard is opening.</td></tr>
  * <tr><td>OPEN</td><td>The shard is open.</td></tr>
  * <tr><td>OPENING_ERROR</td><td>An error during the opening of the shard.</td></tr>
  * <tr><td>CLOSING</td><td>In the process of closing.</td></tr>
  * <tr><td>CLOSED</td><td>The shard is closed.</td></tr>
  * <tr><td>CLOSING_ERROR</td><td>An error during the closing of the shard.</td></tr>
  * </table>
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
 * The user object is used to pass user context to server
 * side session.
 */
struct User {
  /** username. */
  1:string username,
  /** map of user attributes. */
  2:map<string,string> attributes
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
//  3:i32 recordCount
}

/**
 * The Query object holds the query string (normal Lucene syntax), 
 * filters and type of scoring (used when super query is on).
 */
struct Query {
  /**
   * A Lucene syntax based query.
   */
  1:string query,
  /**
   * If the Row query is on, meaning the query will be perform against all the 
   * Records (joining records in some cases) and the result will be Rows (groupings of Record).
   */
  2:bool rowQuery = 1,
  /**
   * The scoring type, see the document on ScoreType for explanation of each score type.
   */
  3:ScoreType scoreType = ScoreType.SUPER, 
  /**
   * The Row filter (normal Lucene syntax), is a filter performed 
   * after the join to filter out entire Rows from the results.  This
   * field is ignored when rowQuery is false.
   */
  4:string rowFilter,
  /**
   * The Record filter (normal Lucene syntax), is a filter performed 
   * before the join to filter out Records from the results.
   */
  5:string recordFilter
}

/**
 * The HighlightOptions controls how the data is fetched and returned.
 */
struct HighlightOptions {
  /**
   * The original query is required if used in the Blur.fetchRow call.  If 
   * the highlightOptions is used in a call to Blur.query then the Query 
   * passed into the call via the BlurQuery will be used if this query is 
   * null.  So that means if you use highlighting from the query call you can 
   * leave this attribute null and it will default to the normal behavior.
   */
  1:Query query,

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
   * The column families to fetch. If null, fetch all. If empty, fetch none.
   */
  5:set<string> columnFamiliesToFetch,
  /**
   * The columns in the families to fetch. If null, fetch all. If empty, fetch none.
   */
  6:map<string,set<string>> columnsToFetch,
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
   * would be 100.  Used in conjunction with startRecord. By default this will fetch the first 
   * 1000 records of the row.
   */
  9:i32 maxRecordsToFetch = 1000,
  /**
   * The HighlightOptions object controls how the data is highlighted.  If null no highlighting will occur.
   */
  10:HighlightOptions highlightOptions,
  /**
   * Can be null, if provided the provided family order will be the order in which the families are returned.
   */
  11:list<string> orderOfFamiliesToFetch
}

/**
 * FetchRowResult contains row result from a fetch.
 */
struct FetchRowResult {
  /**
   * The row fetched.
   */
  1:Row row,
  /**
   * See Selector startRecord.
   */
  2:i32 startRecord = -1,
  /**
   * See Selector maxRecordsToFetch.
   */
  3:i32 maxRecordsToFetch = -1,
  /**
   * Are there more Records to fetch based on the Selector provided.
   */
  4:bool moreRecordsToFetch = 0,
  /**
   * The total number of records the Selector found.
   */
  5:i32 totalRecords
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
 * FetchResult contains the row or record fetch result based if the Selector 
 * was going to fetch the entire row or a single record.
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
 * Blur facet.
 */
struct Facet {
  /** The facet query. */
  1:string queryStr,
  /** The minimum number of results before no longer processing the facet.  This 
      is a good way to decrease the strain on the system while using many facets. For 
      example if you set this attribute to 1000, then the shard server will stop 
      processing the facet at the 1000 mark.  However because this is processed at 
      the shard server level the controller will likely return more than the minimum 
      because it sums the answers from the shard servers.
   */
  2:i64 minimumNumberOfBlurResults = 9223372036854775807
}

struct SortField {
  1:string family,
  2:string column,
  3:bool reverse
}

/**
 * The Blur Query object that contains the query that needs to be executed along 
 * with the query options.
 */
struct BlurQuery {
  /**
   * The query information.
   */
  1:Query query,
  /**
   * A list of Facets to execute with the given query.
   */
  3:list<Facet> facets,
  /**
   * Selector is used to fetch data in the search results, if null only location ids will be fetched.
   */
  4:Selector selector,
  /**
   * Enabled by default to use a cached result if the query matches a previous run query with the 
   * configured amount of time.
   */
  6:bool useCacheIfPresent = 1,
  /**
   * The starting result position, 0 by default.
   */
  7:i64 start = 0,
  /**
   * The number of fetched results, 10 by default.
   */
  8:i32 fetch = 10, 
  /**
   * The minimum number of results to find before returning.
   */
  9:i64 minimumNumberOfResults = 9223372036854775807,
  /**
   * The maximum amount of time the query should execute before timing out.
   */
  10:i64 maxQueryTime = 9223372036854775807,
  /**
   * Sets the uuid of this query, this is normal set by the client so that the status 
   * of a running query can be found or the query can be canceled.
   */
  11:string uuid,
  /**
   * Sets a user context, only used for logging at this point.
   * @Deprecated use setUser method on Blur service.
   */
  12:string userContext,
  /**
   * Enabled by default to cache this result.  False would not cache the result.
   */
  13:bool cacheResult = 1,
  /**
   * Sets the start time, if 0 the controller sets the time.
   */
  14:i64 startTime = 0,
  /**
   * The sortfields are applied in order to sort the results.
   */
  15:list<SortField> sortFields,
  /**
   * Optional optimization for record queries to run against a single row.  This will allow the query to be executed on one and only one shard in the cluster.
   */
  16:string rowId
}

/**
 * Carries the one value from the sort that allows the merging of results.
 */
union SortFieldResult {
  /**
   * Carries the null boolean incase the field is null.
   */
  1:bool nullValue,
  /**
   * The string value.
   */
  2:string stringValue,
  /**
   * The integer value.
   */
  3:i32 intValue,
  /**
   * The long value.
   */
  4:i64 longValue,
  /**
   * The double value.
   */
  5:double doubleValue,
  /**
   * The binary value.
   */
  6:binary binaryValue
}

/**
 * The BlurResult carries the score, the location id and the fetched result (if any) form each query.
 */
struct BlurResult {
  /**
   * WARNING: This is an internal only attribute and is not intended for use by clients.
   */
  1:string locationId,
  /**
   * The score for the hit in the query.
   */
  2:double score,
  /**
   * The fetched result if any.
   */
  3:FetchResult fetchResult,
  /**
   * The fields used for sorting.
   */
  4:list<SortFieldResult> sortFieldResults
}

/**
 * BlurResults holds all information resulting from a query.
 */
struct BlurResults {
  /**
   * The total number of hits in the query.
   */
  1:i64 totalResults = 0,
  /**
   * Hit counts from each shard in the table.
   */
  2:map<string,i64> shardInfo,
  /**
   * The query results.
   */
  3:list<BlurResult> results,
  /**
   * The faceted count.
   */
  4:list<i64> facetCounts,
  /**
   * Not currently used, a future feature could allow for partial results with errors.
   */
  5:list<BlurException> exceptions,
  /**
   * The original query.
   */
  6:BlurQuery query
}

/**
 * The RowMutation defines how the given Record is to be mutated.
 */
struct RecordMutation {
  /**
   * Define how to mutate the given Record.
   */
  1:RecordMutationType recordMutationType = RecordMutationType.REPLACE_ENTIRE_RECORD,
  /**
   * The Record to mutate.
   */
  2:Record record
}

/**
 * The RowMutation defines how the given Row is to be mutated.
 */
struct RowMutation {
  /**
   * The table that the row mutation is to act upon.
   */
  1:string table,
  /**
   * The row id that the row mutation is to act upon.
   */
  2:string rowId,
  /**
   * The RowMutationType to define how to mutate the given Row.
   */
  4:RowMutationType rowMutationType = RowMutationType.REPLACE_ROW,
  /**
   * The RecordMutations if any for this Row.
   */
  5:list<RecordMutation> recordMutations
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
  6:string uuid,
  /**
   * The status of the query NOT_FOUND if uuid is not found else FOUND
   */
  7:Status status,
  /**
   * The user executing the given query.
   */
  8:User user
}

/**
 * TableStats holds the statistics for a given table.
 */
struct TableStats {
  /**
   * The table name.
   */
  1:string tableName,
  /**
   * The size in bytes.
   */
  2:i64 bytes,
  /**
   * The record count.
   */
  3:i64 recordCount,
  /**
   * The row count.
   */
  4:i64 rowCount,
  /**
   * The number of pending segment imports for this table.
   */
  5:i64 segmentImportPendingCount = 0,
  /**
   * The number of segment imports in progress for this table.
   */
  6:i64 segmentImportInProgressCount = 0
}

/**
 * The ColumnDefinition defines how a given Column should be interpreted (indexed/stored)
 */
struct ColumnDefinition {
  /**
   * Required. The family that this column exists within.
   */
  1:string family,
  /**
   * Required. The column name.
   */
  2:string columnName,
  /**
   * If this column definition is for a sub column then provide the sub column name.  Otherwise leave this field null.
   */
  3:string subColumnName,
  /**
   * If this column should be searchable without having to specify the name of the column in the query.  
   * NOTE: This will index the column as a full text field in a default field, so that means it's going to be indexed twice.
   */
  4:bool fieldLessIndexed,
  /**
   * The field type for the column.  The built in types are:
   * <ul>
   * <li>text - Full text indexing.</li>
   * <li>string - Indexed string literal</li>
   * <li>int - Converted to an integer and indexed numerically.</li>
   * <li>long - Converted to an long and indexed numerically.</li>
   * <li>float - Converted to an float and indexed numerically.</li>
   * <li>double - Converted to an double and indexed numerically.</li>
   * <li>stored - Not indexed, only stored.</li>
   * </ul>
   */
  5:string fieldType,
  /**
   * For any custom field types, you can pass in configuration properties.
   */
  6:map<string, string> properties,
  /**
   * This will attempt to enable sorting for this column, if the type does not support sorting then an exception will be thrown.
   */
  7:bool sortable,
  /**
   * This will attempt to enable the ability for multiple values per column name in a single Record.
   */
  8:optional bool multiValueField = 1
}

/**
 * The current schema of the table.
 */
struct Schema {
  /**
   * The table name.
   */
  1:string table,
  /**
   * Families and the column definitions within them.
   */
  2:map<string,map<string,ColumnDefinition>> families
}

/**
 * The table descriptor defines the base structure of the table as well as properties need for setup.
 */
struct TableDescriptor {
  /**
   * Is the table enabled or not, enabled by default.
   */
  1:bool enabled = 1,
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
   * The files extensions that you would like to allow block cache to cache.  If null (default) everything is cached.
   */
  11:set<string> blockCachingFileTypes,
  /**
   * If a table is set to be readonly, that means that mutates through Thrift are NOT allowed.  However 
   * updates through MapReduce are allowed and in fact they are only allowed if the table is in readOnly mode.
   */
  12:bool readOnly = 0,
  /**
   * This is a list of fields to prefetch into the blockcache.  The format of the entries should 
   * be family dot column, "family.column".
   */
  13:list<string> preCacheCols
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

/**
 * The Metric will hold all the information for a given Metric.
 */
struct Metric {
  /** metric name. */
  1:string name,
  /** map of string values emitted by the Metric. */
  2:map<string,string> strMap,
  /** map of long values emitted by the Metric. */
  3:map<string,i64> longMap,
  /** map of double values emitted by the Metric. */
  4:map<string,double> doubleMap
}

/**
 * Logging level enum used to change the logging levels at runtime.
 */
enum Level {
  OFF,
  FATAL,
  ERROR,
  WARN,
  INFO,
  DEBUG,
  TRACE,
  ALL
}

union Value {
  1:string stringValue,
  2:i32 intValue,
  3:i16 shortValue,
  4:i64 longValue,
  5:double doubleValue,
  6:double floatValue,
  7:binary binaryValue,
  8:bool booleanValue,
  9:bool nullValue
}

struct Shard {
  1:string table,
  2:string shard
}

struct Server {
  1:string server
}

enum BlurObjectType {
  MAP, LIST, NAME, VALUE
}

struct BlurPackedObject {
  1:i32 parentId,
  2:BlurObjectType type,
  3:Value value,
}

union ValueObject {
  1:Value value,
  2:list<BlurPackedObject> blurObject
}

union Response {
  1:map<Shard, ValueObject> shardToValue,
  2:map<Server, ValueObject> serverToValue,
  3:ValueObject value
}

struct Arguments {
  1:map<string, ValueObject> values
}

enum CommandStatusState {
  RUNNING,
  INTERRUPTED,
  COMPLETE,
  BACK_PRESSURE_INTERRUPTED
}

struct CommandStatus {
  1:string executionId,
  2:string table,
  3:string commandName,
  4:Arguments arguments,
  5:CommandStatusState state
}

struct ArgumentDescriptor {
  1:string name,
  2:string type,
  3:string description
}

struct CommandDescriptor {
  1:string commandName,
  2:string description,
  3:map<string, ArgumentDescriptor> requiredArguments,
  4:map<string, ArgumentDescriptor> optionalArguments,
  5:string returnType,
  6:string version
}

struct CommandTarget {
  1:set<string> tables,
  2:set<string> shards
}

struct CommandRequest {
  1:string name,
  2:CommandTarget target
}

/**
 * The Blur service API.  This API is the same for both controller servers as well as 
 * shards servers.  Each of the methods are documented.
 */
service Blur {

  // Platform Commands

  /**
   * List the currently installed commands in the server process.
   */
  list<CommandDescriptor> listInstalledCommands() throws (1:BlurException ex)

  /**
   * Executes the given command by name on the table with the provided arguments.
   */
  Response execute(1:string commandName, 2:Arguments arguments) throws (1:BlurException bex, 2:TimeoutException tex)

  /**
   * If the execute command times out due to command taking longer than the configured 
   * network tcp timeout this method allows the client to reconnect to the already 
   * executing command.
   */
  Response reconnect(1:i64 instanceExecutionId) throws (1:BlurException bex, 2:TimeoutException tex)

  /**
   * Fetches the command status ids in the order they were submitted.
   */
  list<string> commandStatusList(1:i32 startingAt, 2:i16 fetch, 3:CommandStatusState state) throws (1:BlurException ex)

  /**
   * Retrieves the command status by the given command execution id.
   */
  CommandStatus commandStatus(1:string commandExecutionId) throws (1:BlurException ex)

  /**
   * Cancels the command with the given command execution id.
   */
  void commandCancel(1:string commandExecutionId) throws (1:BlurException ex)

  /**
   * Releases and refreshes the read snapshots of the indexes in the session for the 
   * current connection.
   */
  oneway void refresh()

  /**
   * Executes command.
   */
  oneway void executeCommand(1:CommandRequest commandRequest)

  //Table Commands

  /**
   * Creates a table with the given TableDescriptor.
   */
  void createTable(
    /** the TableDescriptor.  */
    1:TableDescriptor tableDescriptor
  ) throws (1:BlurException ex)

  /**
   * Enables the given table, blocking until all shards are online.
   */
  void enableTable(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Disables the given table, blocking until all shards are offline.
   */
  void disableTable(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Removes the given table, with an optional to delete the underlying index storage as well.
   */
  void removeTable(
    /** the table name. */
    1:string table, 
    /** true to remove the index storage and false if to preserve.*/
    2:bool deleteIndexFiles
  ) throws (1:BlurException ex)

  /**
   * Attempts to add a column definition to the given table.
   * @return true if successfully defined false if not.
   */
  bool addColumnDefinition(
    /** the name of the table. */
    1:string table, 
    /** the ColumnDefinition. */
    2:ColumnDefinition columnDefinition
  ) throws (1:BlurException ex)
  
  /**
   * Returns a list of the table names across all shard clusters.
   * @return list of all tables in all shard clusters.
   */
  list<string> tableList() throws (1:BlurException ex)

  /**
   * Returns a list of the table names for the given cluster.
   * @return list of all the tables within the given shard cluster.
   */
  list<string> tableListByCluster(
    /** the cluster name. */
    1:string cluster
  ) throws (1:BlurException ex)

  /**
   * Returns a table descriptor for the given table.
   * @return the TableDescriptor.
   */
  TableDescriptor describe(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Gets the schema for a given table.
   * @return Schema.
   */
  Schema schema(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Parses the given query and returns the string that represents the query.
   * @return string representation of the parsed query.
   */
  string parseQuery(
    /** the table name. */
    1:string table, 
    /** the query to parse. */
    2:Query query
  ) throws (1:BlurException ex)

  /**
   * Gets the table stats for the given table.
   * @return TableStats.
   */
  TableStats tableStats(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Will perform a forced optimize on the index in the given table.
   */
  void optimize(
    /** table the name of the table. */
    1:string table, 
    /** the maximum of segments per shard index after the operation is completed. */
    2:i32 numberOfSegmentsPerShard
  ) throws (1:BlurException ex)

  /**
   * Creates a snapshot for the table with the given name
   */
  void createSnapshot(1:string table, 2:string name) throws (1:BlurException ex)

  /**
   * Removes a previous snapshot(identified by name) of the table
   */
  void removeSnapshot(1:string table, 2:string name) throws (1:BlurException ex)

  /**
   * Returns a map where the key is the shard, and the list is the snapshots within that shard
   */
  map<string,list<string>> listSnapshots(1:string table) throws (1:BlurException ex)

  // Data Commands

  /**
   * Sets the User for the current session.
   */
  oneway void setUser(
    /** the User object. */
    1:User user
  )

  /**
   * Executes a query against a the given table and returns the results.  If this method is 
   * executed against a controller the results will contain the aggregated results from all 
   * the shards.  If this method is executed against a shard server the results will only 
   * contain aggregated results from the shards of the given table that are being served on 
   * the shard server, if any.
   * @return the BlurResults.
   */
  BlurResults query(
    /** the table name. */
    1:string table, 
    /** the query to execute. */
    2:BlurQuery blurQuery
  ) throws (1:BlurException ex)

  /**
   * Fetches a Row or a Record in the given table with the given Selector.
   * @return the FetchResult.
   */
  FetchResult fetchRow(
    /** the table name. */
    1:string table, 
    /** the Selector to use to fetch the Row or Record. */
    2:Selector selector
  ) throws (1:BlurException ex)

  /**
   * Fetches a batch of Rows or Records in the given table with the given Selector list.
   * @return the FetchResult.
   */
  list<FetchResult> fetchRowBatch(
    /** the table name. */
    1:string table, 
    /** the Selector to use to fetch the Row or Record. */
    2:list<Selector> selectors
  ) throws (1:BlurException ex)

  /**
   * Loads data from external location.
   */
  void loadData(
     /** The table name. */
     1:string table,
     /** Location of bulk data load. */
     2:string location
  ) throws (1:BlurException ex)

  void validateIndex(1:string table, 2:list<string> externalIndexPaths) throws (1:BlurException ex)

  void loadIndex(1:string table, 2:list<string> externalIndexPaths) throws (1:BlurException ex)

  /**
   * Mutates a Row given the RowMutation that is provided.
   */
  void mutate(
    /** the RowMutation.*/
    1:RowMutation mutation
  ) throws (1:BlurException ex)

  /**
   * Enqueue a RowMutation. Note that the effect of the RowMutation will occur at some point in the future, volume and load will play a role in how much time will pass before the mutation goes into effect.
   */
  void enqueueMutate(
    /** the RowMutation.*/
    1:RowMutation mutation
  ) throws (1:BlurException ex)

  /**
   * Mutates a group of Rows given the list of RowMutations that are provided.  Note: This is not an atomic operation.
   */
  void mutateBatch(
    /** the batch of RowMutations.*/
    1:list<RowMutation> mutations
  ) throws (1:BlurException ex)

  /**
   * Enqueue a batch of RowMutations. Note that the effect of the RowMutation will occur at some point in the future, volume and load will play a role in how much time will pass before the mutation goes into effect.
   */
  void enqueueMutateBatch(
    /** the batch of RowMutations.*/
    1:list<RowMutation> mutations
  ) throws (1:BlurException ex)

  /**
   * Starts a transaction for update (e.g. Mutate).  Returns a transaction id.
   */
  void bulkMutateStart(
    /** The bulk id. */
    1:string bulkId
  ) throws (1:BlurException ex)

  /**
   * Adds to the specified transaction.
   */
  void bulkMutateAdd(
    /** The bulk id. */
    1:string bulkId, 
    /** The row mutation. */
    2:RowMutation rowMutation
  ) throws (1:BlurException ex)

  /**
   * Adds to the specified transaction.
   */
  void bulkMutateAddMultiple(
    /** The bulk id. */
    1:string bulkId, 
    /** The row mutation. */
    2:list<RowMutation> rowMutations
  ) throws (1:BlurException ex)

  /**
   * Finishes the bulk mutate.  If apply is true the mutations are applied and committed.  If false the bulk mutate is deleted and not applied.
   */
  void bulkMutateFinish(
    /** The bulk id. */
    1:string bulkId,
    /** Apply the bulk mutate flag. */
    2:bool apply,
    /** If true this call will not block on bulk completion.  This may be required for loader bulk loads. */
    3:bool blockUntilComplete
  ) throws (1:BlurException ex)

  /**
   * Cancels a query that is executing against the given table with the given uuid.  Note, the 
   * cancel call maybe take some time for the query actually stops executing.
   */
  void cancelQuery(
    /** the table name. */
    1:string table, 
    /** the uuid of the query. */
    2:string uuid
  ) throws (1:BlurException ex)

  /**
   * Returns a list of the query ids of queries that have recently been executed for the given table.
   * @return list of all the uuids of the queries uuids.
   */
  list<string> queryStatusIdList(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Returns the query status for the given table and query uuid.
   * @return fetches the BlurQueryStatus for the given table and uuid.
   */
  BlurQueryStatus queryStatusById(
    /** the table name. */
    1:string table, 
    /** the uuid of the query. */
    2:string uuid
  ) throws (1:BlurException ex)

  /**
   * Gets the terms list from the index for the given table, family, column using the 
   * startWith value to page through the results.  This method only makes sense to use with 
   * string and text field types.
   * @return the list of terms for the given column.
   */
  list<string> terms(
    /** the table name. */
    1:string table, 
    /** the column family. If the frequency requested is a system field like "rowid", "recordid", "family", etc then columnFamily can be null. */
    2:string columnFamily, 
    /** the column name. */
    3:string columnName,
    /** the term to start with assuming that you are paging through the term list. */
    4:string startWith, 
    /** the number to fetch at once. */
    5:i16 size
  ) throws (1:BlurException ex)

  /**
   * Gets the record frequency for the provided table, family, column and value.
   * @return the count for the entire table.
   */
  i64 recordFrequency(
    /** the table name. */
    1:string table, 
    /** the column family. If the frequency requested is a system field like "rowid", "recordid", "family", etc then columnFamily can be null. */
    2:string columnFamily, 
    /** the column name. */
    3:string columnName, 
    /** the value. */
    4:string value
  ) throws (1:BlurException ex)

  // Cluster methods

  /**
   * Returns a list of all the shard clusters.
   * @return list of all the shard clusters.
   */
  list<string> shardClusterList() throws (1:BlurException ex)
  /**
   * Returns a list of all the shard servers for the given cluster.
   * @return list of all the shard servers within the cluster.
   */
  list<string> shardServerList(
    /** the cluster name. */
    1:string cluster
  ) throws (1:BlurException ex)
  /**
   * Returns a list of all the controller servers.
   * @return list of all the controllers.
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
   * @return map of shards in a table to the shard servers.
   */
  map<string,string> shardServerLayout(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)

  /**
   * Returns a map of the layout of the given table, where the key is the shard name and the 
   * value is the shard server.<br><br>
   * This method will return immediately with what shards are currently 
   * open in the shard server.  So if a shard is being moved to another server and is being 
   * closed by this server it WILL be returned in the map.  The shardServerLayout method would not return 
   * the shard given the same situation.
   * @return map of shards to a map of shard servers with the state of the shard.
   */
  map<string,map<string,ShardState>> shardServerLayoutState(
    /** the table name. */
    1:string table
  ) throws (1:BlurException ex)
  
  /**
   * Checks to see if the given cluster is in safemode.
   * @return boolean.
   */
  bool isInSafeMode(
    /** the name of the cluster. */
    1:string cluster
  ) throws (1:BlurException ex)

  /**
   * Fetches the Blur configuration.
   * @return Map of property name to value.
   */
  map<string,string> configuration() throws (1:BlurException ex)

  /**
   * Fetches the Blur configuration.
   * @return Map of property name to value.
   */
  string configurationPerServer(1:string thriftServerPlusPort, 2:string configName) throws (1:BlurException ex)

  /**
   * Fetches the Blur metrics by name.  If the metrics parameter is null all the Metrics are returned.
   * @return Map of metric name to Metric.
   */
  map<string,Metric> metrics(
    /** the names of the metrics to return.  If null all are returned. */
    1:set<string> metrics
  ) throws (1:BlurException ex)

  /**
   * Starts a trace with the given trace id.
   */
  oneway void startTrace(
    /** the trace id. */
    1:string traceId,
    /** the request id, used to connected remote calls together.  Client can pass null. */
    2:string requestId
  )

  /**
   * Get a list of all the traces.
   * @return the list of trace ids.
   */
  list<string> traceList() throws (1:BlurException ex)

  /**
   * Gets a request list for the given trace.
   * @return the list of request ids for the given trace id.
   */
  list<string> traceRequestList(
    /** the trace id. */
    1:string traceId
  ) throws (1:BlurException ex)

  /**
   * Fetches the given trace.
   * @return the json for the given trace request.
   */
  string traceRequestFetch(
    /** the trace id. */
    1:string traceId, 
    /** the request id. */
    2:string requestId
  ) throws (1:BlurException ex)

  /**
   * Remove the trace for the given trace id.
   */
  void traceRemove(
    /** the trace id. */
    1:string traceId
  ) throws (1:BlurException ex)

  /** 
   * A way to ping a server to make sure the connection is still valid.
   */
  void ping()

  /**
   * Changes the logging level for the given instance dynamically at runtime.
   */
  void logging(
    /** the className or Logger Name of the Logger to be changed. */
    1:string classNameOrLoggerName, 
    /** the logging level. */
    2:Level level
  ) throws (1:BlurException ex)

  /**
   * Resets the logging for this instance to match the log4j file.  NOTE: This will allow for dynamically changing to logging file at runtime.
   */
  void resetLogging() throws (1:BlurException ex)

}


