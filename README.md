Blur
====

Blur is a NoSQL data store built on top of Lucene, Hadoop, Thrift, and Zookeeper.  Tables consist of a series of shards (Lucene indexes) that are distributed across a cluster of commodity servers.

Getting Started
----

### Clone

First clone the project and compile the project using Maven.  Once this is complete the blur libraries and dependences will be copied into the lib directory.

### Zookeeper

Setup [Zookeeper][Zookeeper].

### HDFS

It is assumed that all your servers will be setup to run Hadoop's HDFS filesystem.  Though possible, the Map/Reduce system is not recommended to be run on the same machines as blur.  Follow the Hadoop [cluster setup][cluster_setup] guide.

NOTE: If you are running blur on a single machine this is not necessary, but [passphraseless][single_node] ssh is still needed.

### blur-env.sh

Next you will need to configure the `config/blur-env.sh` file.

    export JAVA_HOME=/usr/lib/j2sdk1.6-sun
    export HADOOP_HOME=/var/hadoop-0.20.2

### blur.properties

Then you will need to setup the `config/blur.properties` file.

    blur.zookeeper.connection=localhost
    blur.local.cache.pathes=/tmp/blur-cache
    blur.cluster.name=default

### shards

Then in the `config/shards` list the servers that should run as blur shard servers.

    shard1
    shard2
    shard3

### controllers

Like the shards file, in the `config/controllers` list servers that will run as the blur controller servers.

    controller1
    controller2

NOTE: To just get started you do not need to run controllers as the shard servers are fully functional on the their own.  Both the controllers and the shard servers share the same thrift API.

### $BLUR_HOME

It is a good idea to add `export BLUR_HOME=/var/blur` in your `.bash_profile`.

### Setup Nodes

Copy the Blur directory to the same location on all servers in the cluster.

Running Blur
----

### Start

To start the entire cluster run `bin/start-all.sh`, this will execute `bin/start-shards.sh` and then `bin/start-controllers.sh`.  These two scripts start blur on all the servers.

### Stop

To shutdown blur run `bin/stop-all.sh`, this will stop all the blur processes on all the servers.

Creating a Table
----

### Standalone mode

If you are running on a single node you may reference a local directory for storing the index data.

    AnalyzerDefinition ad = new AnalyzerDefinition();
    TableDescriptor td = new TableDescriptor(); 
    td.setTableUri("file:///tmp/blur-tables/test-table"); // Location on the local machine
    td.setAnalyzerDefinition(ad);
    client.createTable("test-table", td);

### Cluster mode

If you are running on a single node you may reference a local directory for storing the index data.

    AnalyzerDefinition ad = new AnalyzerDefinition();
    TableDescriptor td = new TableDescriptor();
    td.setShardCount(16); // The number of shards should be based on how many indexes your hardware can support as well as the volume of data.
    td.setTableUri("hdfs://hadoop-namenode:9000/blur/tables/test-table"); // Location in HDFS
    td.setAnalyzerDefinition(ad);
    client.createTable("test-table", td);

Loading Data
----

### Thrift

This is the long thrift way of creating a lot of objects to create a simple row and load into a table.

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("columnname", "value"));

    Record record = new Record();
    record.setRecordId("recordid-5678");
    record.setFamily("column-family");
    record.setColumns(columns);

    RecordMutation recordMutation = new RecordMutation();
    recordMutation.setRecord(record);
    recordMutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);

    List<RecordMutation> recordMutations = new ArrayList<RecordMutation>();
    recordMutations.add(recordMutation);

    RowMutation mutation = new RowMutation();
    mutation.setTable("test-table");
    mutation.setRowId("rowid-1234");
    mutation.setRowMutationType(RowMutationType.REPLACE_ROW);

    mutation.setRecordMutations(recordMutations);
    client.mutate(mutation);

This is the shorter way of creating the same RowMutation.

    import static com.nearinfinity.blur.utils.BlurUtil.*;

    RowMutation mutation = newRowMutation("test-table", "rowid-1234", 
            newRecordMutation("column-family", "recordid-5678", 
                newColumn("columnname", "value")));
    client.mutate(mutation);

### Map/Reduce Bulk Load

Example coming.

Fetching Data
----

Simple example of how to fetch an entire row from a table by rowid:

    Selector selector = new Selector();
    selector.setRowId("rowid-1234");
    FetchResult fetchRow = client.fetchRow("test-table", selector);
    FetchRowResult rowResult = fetchRow.getRowResult();
    Row row = rowResult.getRow();

To select a subset of columns from a column family:

    Set<String> columnNames = new HashSet<String>();
    columnNames.add("columnname");
    selector.putToColumnsToFetch("column-family", columnNames);

To select a all the columns from a subset of column families:

    selector.addToColumnFamiliesToFetch("column-family");

Searching
----

The blur query language is the same as Lucene's [query parser][queryparser] syntax.

### Simple search

The search example will do a full text search for `value` in each column in every column family.  This is a result of the basic setup, so this behavior can be configured.

    BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr("value");
    blurQuery.setSimpleQuery(simpleQuery);
    blurQuery.setSelector(new Selector());

    BlurResults blurResults = client.query("test-table", blurQuery);
    for (BlurResult result : blurResults.getResults()) {
       System.out.println(result);
    }

Shorted version of the same thing:

    import static com.nearinfinity.blur.utils.BlurUtil.*;

    BlurQuery blurQuery = newSimpleQuery("value");
    BlurResults blurResults = client.query("test-table", blurQuery);
    for (BlurResult result : blurResults.getResults()) {
       System.out.println(result);
    }

The data loaded in the Loading Data section above put `value` in the `columnname` column in the `column-family` column family.  So you could also search for the row by using the `column-family.columnname:value` and find all the rows that contain `value` in `columnname`.

### Expert Search

Example coming.

[cluster_setup]: http://hadoop.apache.org/common/docs/r0.20.203.0/cluster_setup.html
[single_node]: http://hadoop.apache.org/common/docs/r0.20.203.0/single_node_setup.html#Setup+passphraseless
[Zookeeper]: http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html
[queryparser]: http://lucene.apache.org/java/3_3_0/queryparsersyntax.html