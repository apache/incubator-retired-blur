namespace java com.nearinfinity.blur.thrift.generated

struct Hit {
  1:string id,
  2:double score,
  3:string reason = "UNKNOWN"
}

struct Hits {
  1:i64 totalHits = 0,
  2:map<string,i64> shardInfo,
  3:list<Hit> hits
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

struct SuperColumn {
  1:string id,
  2:map<string,Column> columns
}

struct SuperColumnFamily {
  1:string name,
  2:map<string,SuperColumn> superColumns
}

struct Row {
  1:string id,
  2:map<string,SuperColumnFamily> superColumnFamilies
}

enum ScoreType {
  SUPER,
  AGGREGATE,
  BEST,
  CONSTANT
}

exception BlurException {
  1:string message
}

service Blur {

list<string> tableList() throws (1:BlurException ex)
TableDescriptor describe(1:string table) throws (1:BlurException ex)

void enable(1:string table) throws (1:BlurException ex)
void disable(1:string table) throws (1:BlurException ex)
void create(1:string table, 2:TableDescriptor desc) throws (1:BlurException ex)
void drop(1:string table) throws (1:BlurException ex)

bool removeRow(1:string table, 2:string id) throws (1:BlurException ex)
bool removeSuperColumn(1:string table, 2:string id, 3:string superColumnId) throws (1:BlurException ex)
bool replaceRow(1:string table, 2:Row row) throws (1:BlurException ex)
bool appendRow(1:string table, 2:Row row) throws (1:BlurException ex)

Row fetchRow(1:string table, 2:string id) throws (1:BlurException ex)
SuperColumn fetchSuperColumn(1:string table, 2:string id, 3:string superColumnFamilyName, 4:string superColumnId) throws (1:BlurException ex)

Hits search(1:string table, 2:string query, 3:bool superQueryOn, 4:ScoreType type, 5:string filter, 6:i64 start, 7:i32 fetch, 8:i64 minimumNumberOfHits, 9:i64 maxQueryTime)
   throws (1:BlurException ex)

list<string> getDynamicTerms(1:string table) throws (1:BlurException ex)
string getDynamicTermQuery(1:string table, 2:string term) throws (1:BlurException ex)
bool isDynamicTermQuerySuperQuery(1:string table, 2:string term) throws (1:BlurException ex)
void createDynamicTermQuery(1:string table, 2:string term, 3:string query, 4:bool superQueryOn) throws (1:BlurException ex)
void deleteDynamicTermQuery(1:string table, 2:string term) throws (1:BlurException ex)

}
