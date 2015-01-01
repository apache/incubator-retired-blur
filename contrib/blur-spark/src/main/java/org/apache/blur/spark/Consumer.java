package org.apache.blur.spark;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.mapreduce.lib.BlurMapReduceUtil;
import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.mapreduce.lib.BlurOutputCommitter;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.DefaultBlurReducer;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import consumer.kafka.KafkaConfig;
import consumer.kafka.MessageAndMetadata;
import consumer.kafka.client.KafkaReceiver;

/*
 * This Consumer uses Spark RDD saveAsNewAPIHadoopFile API to index BlurMutate
 */

public class Consumer implements Serializable {

	private static final long serialVersionUID = 4332618245650072140L;
	private Properties _props;
	private KafkaConfig _kafkaConfig;

	public void start() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		_kafkaConfig = new KafkaConfig(_props);
		run();
	}

	private void init(String[] args) throws Exception {

		Options options = new Options();
		this._props = new Properties();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		OptionBuilder.withArgName("property=value");
		OptionBuilder.hasArgs(2);
		OptionBuilder.withValueSeparator();
		OptionBuilder.withDescription("use value for given property");
		options.addOption(OptionBuilder.create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption('p')) {
			this._props.load(ClassLoader.getSystemClassLoader()
					.getResourceAsStream(cmd.getOptionValue('p')));
		}
		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			this._props.load(fStream);
		}
		this._props.putAll(cmd.getOptionProperties("D"));

	}

	private void run() {

		String checkpointDirectory = "hdfs://10.252.5.113:9000/user/hadoop/spark";

		// number of partition for Kafka Topic

		int _partitionCount = 5;

		List<JavaDStream<MessageAndMetadata>> streamsList = new ArrayList<JavaDStream<MessageAndMetadata>>(
				_partitionCount);
		JavaDStream<MessageAndMetadata> unionStreams;

		SparkConf conf = new SparkConf().setAppName("KafkaReceiver").set(
				"spark.streaming.blockInterval", "200");

		// Path to Blur Libraries . Can be copied to each Node of Spark Cluster.

		conf.set("spark.executor.extraClassPath",
				"/home/apache-blur-0.2.4/lib/*");

		// Used KryoSerializer for BlurMutate and Text.
		conf.set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");

		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(
				3000));

		/*
		 * Receive Kafka Stream. Create individual Receivers for each Topic
		 * Partition
		 */

		for (int i = 0; i < _partitionCount; i++) {

			streamsList.add(ssc.receiverStream(new KafkaReceiver(_props, i)));

		}

		/*
		 * Union all the streams if there is more than 1 stream
		 */

		if (streamsList.size() > 1) {
			unionStreams = ssc.union(streamsList.get(0),
					streamsList.subList(1, streamsList.size()));
		} else {
			// Otherwise, just use the 1 stream
			unionStreams = streamsList.get(0);
		}

		/*
		 * Generate JavaPairDStream
		 */

		JavaPairDStream<Text, BlurMutate> pairDStream = unionStreams
				.mapToPair(new PairFunction<MessageAndMetadata, Text, BlurMutate>() {

					private static final long serialVersionUID = 443235214978L;

					public Tuple2<Text, BlurMutate> call(
							MessageAndMetadata mmeta) {

						/*
						 * create the BlurMutate from MessageAndMetadata
						 */

						String message = new String(mmeta.getPayload());
						String keyStr = DigestUtils.shaHex(message);
						Text key = new Text((keyStr).getBytes());
						BlurMutate mutate = new BlurMutate(
								BlurMutate.MUTATE_TYPE.REPLACE, keyStr, keyStr,
								"family");
						mutate.addColumn("message", message);

						return new Tuple2<Text, BlurMutate>(key, mutate);
					}
				});

		pairDStream
				.foreachRDD(new Function2<JavaPairRDD<Text, BlurMutate>, Time, Void>() {

					private static final long serialVersionUID = 88875777435L;

					@Override
					public Void call(JavaPairRDD<Text, BlurMutate> rdd,
							Time time) throws Exception {

						/*
						 * Blur Table Details
						 */
						TableDescriptor tableDescriptor = new TableDescriptor();
						String tableUri = new Path(
								"hdfs://10.252.5.113:9000/blur/tables/nrt")
								.toString();
						tableDescriptor.tableUri = tableUri;
						tableDescriptor.cluster = "pearson";
						tableDescriptor.name = "nrt";
						tableDescriptor.shardCount = 9;
						Configuration conf = new Configuration();

						/*
						 * Partition RDD to match Blur Table Shard Count. Used
						 * Custom Partitioner to channel correct BlurMutate to
						 * correct Shard.
						 */

						final JavaPairRDD<Text, BlurMutate> pRdd = rdd
								.partitionBy(
										new BlurSparkPartitioner(
												tableDescriptor.shardCount))
								.persist(StorageLevel.MEMORY_ONLY_2());

						/*
						 * Blur specific Configuration
						 */

						BlurOutputFormat.setIndexLocally(conf, false);
						BlurOutputFormat.setOptimizeInFlight(conf, false);
						conf.setClass("mapreduce.reduce.class",
								DefaultBlurReducer.class, Reducer.class);
						conf.setClass("mapreduce.outputformat.class",
								BlurOutputFormat.class, OutputFormat.class);
						conf.setClass("mapreduce.partitioner.class",
								BlurPartitioner.class, Partitioner.class);
						conf.set("mapred.output.committer.class",
								BlurOutputCommitter.class.getName());
						conf.setInt("blur.output.max.document.buffer.size",
								10000);

						BlurOutputFormat.setTableDescriptor(conf,
								tableDescriptor);

						JobConf jobConf = new JobConf(conf);

						jobConf.setNumReduceTasks(tableDescriptor.shardCount);
						jobConf.setOutputKeyClass(Text.class);
						jobConf.setOutputValueClass(BlurMutate.class);

						BlurMapReduceUtil.addAllJarsInBlurLib(conf);
						BlurMapReduceUtil
								.addDependencyJars(
										conf,
										org.apache.zookeeper.ZooKeeper.class,
										org.apache.lucene.codecs.lucene42.Lucene42Codec.class,
										jobConf.getOutputKeyClass(),
										jobConf.getOutputValueClass());

						/*
						 * Write the RDD to Blur Table
						 */

						if (pRdd.count() > 0)
							pRdd.saveAsNewAPIHadoopFile(tableUri, Text.class,
									BlurMutate.class, BlurOutputFormat.class,
									jobConf);

						return null;
					}
				});

		// ssc.checkpoint(checkpointDirectory);
		ssc.start();
		ssc.awaitTermination();
	}

	public static void main(String[] args) throws Exception {

		Consumer consumer = new Consumer();
		consumer.init(args);
		consumer.start();
	}
}
