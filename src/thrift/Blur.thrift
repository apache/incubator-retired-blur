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

struct Hit {
  1:string id,
  2:double score,
  3:string reason = "UNKNOWN"
}

struct Hits {
  1:i64 totalHits = 0,
  2:map<string,i64> shardInfo,
  3:list<Hit> hits,
  4:list<BlurException> exceptions
}

struct TableDescriptor {
  1:bool isEnabled,
  2:string analyzerDef,
  3:string partitionerClass,
  4:list<string> shardNames
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
  2:set<ColumnFamily> columnFamilies,
  3:bool walDisabled 
}

struct FetchResult {
  1:string table,
  2:string id,
  3:Row row,
  4:bool exists
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

service Blur {

  list<string> shardServerList() throws (1:BlurException ex)
  list<string> controllerServerList() throws (1:BlurException ex)

  list<string> tableList() throws (1:BlurException ex)
  TableDescriptor describe(1:string table) throws (1:BlurException ex)

  map<string,string> shardServerLayout(1:string table) throws (1:BlurException ex)

  void enable(1:string table) throws (1:BlurException ex)
  void disable(1:string table) throws (1:BlurException ex)
  void create(1:string table, 2:TableDescriptor desc) throws (1:BlurException ex)
  void drop(1:string table) throws (1:BlurException ex)

  void removeRow(1:string table, 2:string id) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
  void replaceRow(1:string table, 2:Row row) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
  void appendRow(1:string table, 2:Row row) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
  FetchResult fetchRow(1:string table, 2:string id) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)

  Hits search(1:string table, 2:SearchQuery searchQuery) throws (1:BlurException be, 2: MissingShardException mse, 3: EventStoppedExecutionException esee)
  void cancelSearch(1:i64 providedUuid) throws (1:BlurException be, 3: EventStoppedExecutionException esee)

  list<string> getDynamicTerms(1:string table) throws (1:BlurException be, 2: MissingShardException mse)
  string getDynamicTermQuery(1:string table, 2:string term) throws (1:BlurException be, 2: MissingShardException mse)
  bool isDynamicTermQuerySuperQuery(1:string table, 2:string term) throws (1:BlurException be, 2: MissingShardException mse)
  void createDynamicTermQuery(1:string table, 2:string term, 3:string query, 4:bool superQueryOn) throws (1:BlurException be, 2: MissingShardException mse)
  void deleteDynamicTermQuery(1:string table, 2:string term) throws (1:BlurException be, 2: MissingShardException mse)

  void shutdownShard(1:string node) throws (1:BlurException be)
  void shutdownController(1:string node) throws (1:BlurException be)

}


