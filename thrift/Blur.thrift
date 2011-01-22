namespace java com.nearinfinity.blur.thrift.generated

exception BlurException {
  1:string message
}

enum ScoreType {
  SUPER,
  AGGREGATE,
  BEST,
  CONSTANT
}

struct SearchQuery {
  1:string queryStr,
  2:bool superQueryOn,
  3:ScoreType type, 
  4:string postSuperFilter,
  5:string preSuperFilter,
  6:i64 start,
  7:i32 fetch, 
  8:i64 minimumNumberOfHits,
  9:i64 maxQueryTime,
  10:i64 uuid
}

struct Hit {
  1:string locationId,
  2:double score,
  3:string reason = "UNKNOWN"
}

struct Hits {
  1:i64 totalHits = 0,
  2:map<string,i64> shardInfo,
  3:list<Hit> hits,
  4:list<BlurException> exceptions,
  5:SearchQuery query,
  6:i64 realTime,
  7:i64 cpuTime
}

struct TableDescriptor {
  1:bool isEnabled,
  2:string analyzerDef,
  3:list<string> shardNames
}

struct Column {
  1:string name,
  2:list<string> values
}

struct ColumnFamily {
  1:string family,
  2:map<string,set<Column>> columns
}

struct Row {
  1:string id,
  2:set<ColumnFamily> columnFamilies
}

struct FetchResult {
  1:bool exists,
  2:bool deleted,
  3:string table,
  4:Row row,
  5:set<Column> record
}

struct Selector {
  1:string locationId,
  2:bool recordOnly,
  3:set<string> columnFamilies,
  4:map<string,set<string>> columns
}

struct Facet {
  1:string queryStr,
  2:i64 minimumNumberOfHits
}

struct FacetQuery {
  1:SearchQuery searchQuery,
  2:list<Facet> facets,
  3:i64 maxQueryTime
}

struct FacetResult {
  1:FacetQuery facetQuery,
  2:map<Facet,i64> counts
}

struct SearchQueryStatus {
  1:SearchQuery query,
  2:Facet facet,
  3:i64 realTime,
  4:i64 cpuTime,
  5:double complete,
  6:bool running,
  7:bool interrupted,
  8:i64 uuid
}

struct Schema {
  1:string table,
  2:map<string,set<string>> columnFamilies
}

service BlurSearch {
  list<string> shardServerList() throws (1:BlurException ex)
  list<string> controllerServerList() throws (1:BlurException ex)
  map<string,string> shardServerLayout(1:string table) throws (1:BlurException ex)

  list<string> tableList() throws (1:BlurException ex)
  TableDescriptor describe(1:string table) throws (1:BlurException ex)

  Hits search(1:string table, 2:SearchQuery searchQuery) throws (1:BlurException be)
  FacetResult facetSearch(1:string table, 2:FacetQuery facetQuery) throws (1:BlurException be)
  void cancelSearch(1:i64 uuid) throws (1:BlurException be)
  list<SearchQueryStatus> currentSearches(1:string table) throws (1:BlurException be)

  Schema schema(1:string table) throws (1:BlurException ex)
  list<string> terms(1:string table, 2:string columnFamily, 3:string columnName, 4:string startWith, 5:i16 size) throws (1:BlurException ex)
  i64 recordFrequency(1:string table, 2:string columnFamily, 3:string columnName, 4:string value) throws (1:BlurException ex)

  FetchResult fetchRow(1:string table, 2:Selector selector) throws (1:BlurException be)
}

