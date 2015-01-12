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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.spark.util.JavaSparkUtil;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.hadoop.conf.Configuration;
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

@SuppressWarnings("serial")
public abstract class BlurLoadSparkProcessor<T> implements Serializable {

  protected static final String MAPRED_OUTPUT_COMMITTER_CLASS = "mapred.output.committer.class";
  protected static final String MAPREDUCE_PARTITIONER_CLASS = "mapreduce.partitioner.class";
  protected static final String SPARK_STREAMING_BLOCK_INTERVAL = "spark.streaming.blockInterval";
  protected static final String SPARK_EXECUTOR_EXTRA_CLASS_PATH = "spark.executor.extraClassPath";
  protected static final String ORG_APACHE_SPARK_SERIALIZER_KRYO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
  protected static final String SPARK_SERIALIZER = "spark.serializer";

  public void run() throws IOException {
    SparkConf conf = new SparkConf();
    conf.setAppName(getAppName());
    conf.set(SPARK_SERIALIZER, ORG_APACHE_SPARK_SERIALIZER_KRYO_SERIALIZER);
    JavaSparkUtil.packProjectJars(conf);
    setupSparkConf(conf);

    JavaStreamingContext ssc = new JavaStreamingContext(conf, getDuration());
    List<JavaDStream<T>> streamsList = getStreamsList(ssc);

    // Union all the streams if there is more than 1 stream
    JavaDStream<T> streams = unionStreams(ssc, streamsList);

    JavaPairDStream<String, RowMutation> pairDStream = streams.mapToPair(new PairFunction<T, String, RowMutation>() {
      public Tuple2<String, RowMutation> call(T t) {
        RowMutation rowMutation = convert(t);
        return new Tuple2<String, RowMutation>(rowMutation.getRowId(), rowMutation);
      }
    });

    pairDStream.foreachRDD(getFunction());

    ssc.start();
    ssc.awaitTermination();
  }

  protected abstract Function2<JavaPairRDD<String, RowMutation>, Time, Void> getFunction();

  private JavaDStream<T> unionStreams(JavaStreamingContext ssc, List<JavaDStream<T>> streamsList) {
    JavaDStream<T> unionStreams;
    if (streamsList.size() > 1) {
      unionStreams = ssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
    } else {
      // Otherwise, just use the 1 stream
      unionStreams = streamsList.get(0);
    }
    return unionStreams;
  }

  protected abstract String getOutputPath();

  /**
   * Gets the storage level for the spark job, default of MEMORY_ONLY_2.
   * 
   * @return
   */
  protected StorageLevel getStorageLevel() {
    return StorageLevel.MEMORY_AND_DISK();
  }

  /**
   * Called just before spark job is executed.
   * 
   * @param configuration
   */
  protected void setupBlurHadoopConfig(Configuration configuration) {

  }

  /**
   * Add custom spark information.
   * 
   * @param conf
   */
  protected void setupSparkConf(SparkConf conf) {

  }

  /**
   * Gets the duration for the batch, default of 10 seconds.
   * 
   * @return
   */
  protected Duration getDuration() {
    return new Duration(10000);
  }

  /**
   * Gets the blur table name to load.
   * 
   * @return
   */
  protected abstract String getBlurTableName();

  /**
   * Gets the blur client for the table.
   * 
   * @return
   */
  protected abstract Iface getBlurClient();

  /**
   * Gets the spark application name.
   * 
   * @return
   */
  protected abstract String getAppName();

  /**
   * Gets the list of streams to load into Blur.
   * 
   * @param ssc
   * 
   * @return
   */
  protected abstract List<JavaDStream<T>> getStreamsList(JavaStreamingContext ssc);

  /**
   * Converts the data into a {@link BlurMutate} object.
   * 
   * @param t
   * @return
   */
  protected abstract RowMutation convert(T t);
}
