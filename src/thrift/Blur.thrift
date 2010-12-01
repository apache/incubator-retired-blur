namespace java com.nearinfinity.blur.thrift.generated

exception BlurException {
  1:string message
}

exception MissingShardException {
  1:string message
}

exception EventStoppedExecutionException {
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
  10:string user,
  11:i64 userUuid,
  12:i64 systemUuid
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

struct SearchQueryStatus {
  1:SearchQuery query,
  2:i64 realTime,
  3:i64 cpuTime,
  4:double complete,
  5:bool running,
  6:bool interrupted
}

service BlurSearch {
  list<string> shardServerList() throws (1:BlurException ex)
  list<string> controllerServerList() throws (1:BlurException ex)
  map<string,string> shardServerLayout(1:string table) throws (1:BlurException ex)

  list<string> tableList() throws (1:BlurException ex)
  TableDescriptor describe(1:string table) throws (1:BlurException ex)

  Hits search(1:string table, 2:SearchQuery searchQuery) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
  void cancelSearch(1:i64 userUuid) throws (1:BlurException be, 2: EventStoppedExecutionException esee)
  list<SearchQueryStatus> currentSearches(1:string table) throws (1:BlurException be)

  FetchResult fetchRow(1:string table, 2:Selector selector) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
}

service BlurBinary extends BlurSearch {
  binary fetchRowBinary(1:string table, 2:string id, 3:binary selector) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
}

service BlurAdmin extends BlurBinary {
  void shutdownShard(1:string node) throws (1:BlurException be)
  void shutdownController(1:string node) throws (1:BlurException be)
}


