Blur
====

Blur is a NoSQL data store built on top of Lucene, Hadoop, Thrift, and Zookeeper.  Tables consist of a series of shards (Lucene indexes) that are distributed across a cluster of commodity servers.

Mail List
----
blur-dev@googlegroups.com or go to http://groups.google.com/group/blur-dev

Getting Started
----

### Clone

First clone the project and compile the project using Maven.  Once this is complete the blur libraries and dependences will be copied into the lib directory.

### Zookeeper Setup

Setup [Zookeeper][Zookeeper].  It is recommended that all production setups use a clustered Zookeeper environment, following best [practices][replicated_zk].

### Hadoop Setup

Blur requires Hadoop to be installed because of library dependencies, but running the Hadoop daemons on the servers is optional.

### HDFS Notes

If you are running Blur on a single machine this is not necessary, but [single node][single_node] setup is still required for libraries.

Setup Hadoop's HDFS filesystem, which is required for clustered setup.  Though possible, the Map/Reduce system is not recommended to be run on the same machines the are running the Blur daemons.  Follow the Hadoop [cluster setup][cluster_setup] guide.

### HDFS Options

HDFS is not required to be installed and running on the same servers as Blur.  However if the source HDFS is being used for heavy Map/Reduce or any other heavy I/O operations, performance could be affected.  The storage location for each table is setup independently and via a URI location (e.g. hdfs://&lt;namenode&gt;:&lt;port&gt;/blur/tables/table/path).  So there may be several tables online in a Blur cluster and each one could reference a different HDFS instance.  This assumes that all the HDFS instances are compatible with one another.
	
NOTE: The normal 0.20.2 is not compatible with Cloudera's 0.20.2 CDH3u2 version.  Meaning you cannot install CDH3 on your Blur servers and reference a normal 0.20.2 HDFS instance for storage (you can not mix these Hadoop versions, and there may be other combinations of CDH and Apache Hadoop that do not work together).  Blur has not been tested with Hadoop version [0.20.203.0][0.20.203.0].

### blur-env.sh Configuration

Next you will need to configure the `config/blur-env.sh` file.  The two exports that are required:

    export JAVA_HOME=/usr/lib/j2sdk1.6-sun
    export HADOOP_HOME=/var/hadoop-0.20.2

### blur.properties Configuration

Then you will need to setup the `config/blur.properties` file.  The default site configuration:

    blur.zookeeper.connection=localhost
    blur.cluster.name=default

Other options:

By default if the `blur.*.hostname` properties are left blank, the default value is the result of `InetAddress.getLocalHost().getHostName();`.  Hostname is required to be unique for every server.
    
    blur.shard.hostname=
    blur.shard.bind.address=0.0.0.0
    blur.shard.bind.port=40020
    blur.shard.server.thrift.thread.count=32
    blur.shard.opener.thread.count=16
    blur.shard.cache.max.querycache.elements=128
    blur.shard.cache.max.timetolive=60000
    blur.shard.filter.cache.class=com.nearinfinity.blur.manager.DefaultBlurFilterCache
    blur.shard.index.warmup.class=com.nearinfinity.blur.manager.indexserver.DefaultBlurIndexWarmup
    blur.shard.index.deletion.policy.class=org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy
    blur.shard.blockcache.direct.memory.allocation=true
    blur.shard.blockcache.slab.count=1
    blur.shard.safemodedelay=60000
    blur.max.clause.count=1024
    blur.indexmanager.search.thread.count=32

    blur.controller.hostname=
    blur.controller.bind.address=0.0.0.0
    blur.controller.bind.port=40010
    blur.controller.server.thrift.thread.count=32
    blur.controller.server.remote.thread.count=64
    blur.controller.remote.fetch.count=100
    blur.controller.cache.max.querycache.elements=128
    blur.controller.cache.max.timetolive=60000

    blur.zookeeper.system.time.tolerance=3000


### shards

Then in the `config/shards` list the servers that should run as blur shard servers.  By default shard servers run on port `40020` and bind to the `0.0.0.0` address.

    shard1
    shard2
    shard3

### controllers

Like the shards file, in the `config/controllers` list servers that will run as the blur controller servers.  By default controller servers run on port `40010` and bind to the `0.0.0.0` address.

    controller1
    controller2

NOTE: If you are going to run a single shard server running controllers is not required.  A single shard server is fully functional on it's own.  Controllers and the shard servers share the same thrift API, so later your code won't have to be modified to run against a cluster.

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

Thrift Client
----

All of the examples below require Thrift to execute, if you have successfully gotten to this point you already have the libraries required.

### Plain Thrift API example

    TTransport trans = new TSocket("controller1", 40010);
    TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
    Client client = new Client(proto);
    try {
        trans.open();
        //use client here
    } catch (Exception e) {
        //do something smart...
    } finally {
        trans.close();
    }

### Automatic connect/pool/error retry API example

    Blur.Iface client = BlurClient.getClient("controller1:40010");
    List<String> tableNames = client.tableList();
	
### Async Thrift client helper API example

    AsyncClientPool pool = new AsyncClientPool(10,60000); // 10 connections per host with a timeout of 60 seconds.
    AsyncIface client = pool.getClient(Blur.AsyncIface.class, "controller1:40010");
	client.tableList(new AsyncMethodCallback<tableList_call>() {
        @Override
        public void onError(Exception exception) {
            //do something smart...
        }  
        @Override
        public void onComplete(tableList_call response) {
            //process result
	    }
    });

Creating a Table
----

### Standalone mode

If you are running on a single node you may reference a local directory for storing the index data.

    AnalyzerDefinition ad = new AnalyzerDefinition();
    
    TableDescriptor td = new TableDescriptor(); 
    td.setTableUri("file:///tmp/blur-tables/test-table"); // Location on the local machine
    td.setAnalyzerDefinition(ad);
    td.setName("test-table");
    
    client.createTable(td);

### Cluster mode

If you are running in a cluster you have to use HDFS as the table storage.  The number of shards should be based on how many indexes your hardware can support as well as the volume of data.

    AnalyzerDefinition ad = new AnalyzerDefinition();
    
    TableDescriptor td = new TableDescriptor();
    td.setShardCount(16);
    td.setTableUri("hdfs://<namenode>:<port>/blur/tables/test-table"); // Location in HDFS
    td.setAnalyzerDefinition(ad);
    td.setName("test-table");
    
    client.createTable(td);

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
    // Driver Class
    public class BlurMapReduce {
      public static void main(String[] args) {
	    Configuration configuration = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: blurindexer <in> <out>");
	      System.exit(2);
	    }
      
        AnalyzerDefinition ad = new AnalyzerDefinition();

        TableDescriptor td = new TableDescriptor();
        td.setShardCount(1);
        td.setTableUri("hdfs://<namenode>:<port>/blur/tables/test-table"); // Location in HDFS
        td.setAnalyzerDefinition(ad);
      
        BlurTask blurTask = new BlurTask();
        blurTask.setTableDescriptor(td);

        // The copy locks are used to throttle how many concurrent 
        // copies from the reducers are occuring at the same time.
        // This is normally needed because the indexing cluster is 
        // typically larger in size than the blur cluster.

        Job job = blurTask.configureJob(configuration);  
        job.setJarByClass(BlurExampleIndexer.class);
        job.setMapperClass(BlurExampleMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1], "job-" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }

      public static class BlurExampleMapper extends BlurMapper<LongWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            record = _mutate.getRecord();
            _mutate.setMutateType(MUTATE_TYPE.ADD);
        }
  	
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          // Reset record
          record.clearColumns();
          
          // Set row id
          record.setRowId("rowid");
          
          // Set record id
          record.setRecordId("recordid");
          
          // Set column family
          record.setFamily("cf1");
          
          // Add a column entry
          record.addColumn(new BlurColumn("column name", "value"));

          // Obviously you would probably parse the value being passed into the method to extract column data.
          
          // Set the key (usually the rowid)
          byte[] bs = record.getRowId().getBytes();
          _key.set(bs, 0, bs.length);
          context.write(_key, _mutate);
          _recordCounter.increment(1);
          context.progress();
        }
      }
    }

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

To select all the columns from a subset of column families:

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
       // do something with the result
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
[single_node]: http://hadoop.apache.org/common/docs/r0.20.203.0/single_node_setup.html
[Zookeeper]: http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html
[queryparser]: http://lucene.apache.org/java/3_3_0/queryparsersyntax.html
[replicated_zk]: http://zookeeper.apache.org/doc/r3.3.3/zookeeperStarted.html#sc_RunningReplicatedZooKeeper
[0.20.203.0]: http://hadoop.apache.org/common/docs/r0.20.203.0/