Blur
=============

Blur is a NoSQL data store built on top of Lucene, Hadoop, Thrift, and Zookeeper.  Tables consist of a series of shards (Lucene indexes) that are distributed across a cluster of commodity servers.

Getting Started
=============

1. First clone the project and compile the project using Maven.  Once this is complete the blur libraries and dependences will be copied into the lib directory.

2. Setup HDFS.  It is assumed that all your servers will be setup to run Hadoop's HDFS filesystem.  Though possible, the Map/Reduce system is not recommended to be run on the same machines as blur.

http://hadoop.apache.org/common/docs/r0.20.203.0/cluster_setup.html

NOTE: If you are running blur on a single machine this is not necessary, but passphraseless ssh is still needed.

http://hadoop.apache.org/common/docs/r0.20.203.0/single_node_setup.html#Setup+passphraseless

3. Next you will need to configure the `config/blur-env.sh` file.

	export JAVA_HOME=/usr/lib/j2sdk1.6-sun
	export HADOOP_HOME=/var/hadoop-0.20.2

4. Then you will need to setup the `config/blur.properties` file.

	blur.zookeeper.connection=localhost
	blur.local.cache.pathes=/tmp/blur-cache
	blur.cluster.name=default

5. Then in the `config/shards` list the servers that should run as blur shard servers.

	shard1
	shard2
	shard3

6. Like the shards file, in the `config/controllers` list servers that will run as the blur controller servers.

	controller1
	controller2

NOTE: To just get started you do not need to run controllers as the shard servers are fully functional on the their own.  Both the controllers and the shard servers share the same thrift API.



