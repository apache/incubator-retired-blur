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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.blur.manager.BlurPartitioner;
import org.apache.blur.mapreduce.lib.BlurColumn;
import org.apache.blur.mapreduce.lib.BlurMutate;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.blur.mapreduce.lib.BlurOutputCommitter;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Time;

import scala.Tuple2;

@SuppressWarnings("serial")
public abstract class BlurMRBulkLoadSparkProcessor<T> extends BlurLoadSparkProcessor<T> {

  @Override
  protected Function2<JavaPairRDD<String, RowMutation>, Time, Void> getFunction() {
    return new Function2<JavaPairRDD<String, RowMutation>, Time, Void>() {
      @Override
      public Void call(JavaPairRDD<String, RowMutation> rdd, Time time) throws Exception {

        // Blur Table Details
        Iface client = getBlurClient();
        TableDescriptor tableDescriptor = client.describe(getBlurTableName());
        Configuration conf = new Configuration();
        // Blur specific Configuration
        conf.setClass(MAPREDUCE_PARTITIONER_CLASS, BlurPartitioner.class, Partitioner.class);
        conf.set(MAPRED_OUTPUT_COMMITTER_CLASS, BlurOutputCommitter.class.getName());

        // Partition RDD to match Blur Table Shard Count. Used Custom
        // Partitioner to channel correct BlurMutate to correct Shard.
        BlurSparkPartitioner blurSparkPartitioner = new BlurSparkPartitioner(tableDescriptor.getShardCount());
        JavaPairRDD<Text, BlurMutate> flatMapToPair = rdd
            .flatMapToPair(new PairFlatMapFunction<Tuple2<String, RowMutation>, Text, BlurMutate>() {
              @Override
              public Iterable<Tuple2<Text, BlurMutate>> call(Tuple2<String, RowMutation> tuple2) throws Exception {
                RowMutation rowMutation = tuple2._2;
                final List<BlurMutate> result = new ArrayList<BlurMutate>();
                List<RecordMutation> recordMutations = rowMutation.getRecordMutations();
                String rowId = rowMutation.getRowId();
                for (RecordMutation recordMutation : recordMutations) {
                  Record record = recordMutation.getRecord();
                  String family = record.getFamily();
                  String recordId = record.getRecordId();
                  List<BlurColumn> columns = toColumns(record.getColumns());

                  BlurRecord blurRecord = new BlurRecord();
                  blurRecord.setRowId(rowId);
                  blurRecord.setFamily(family);
                  blurRecord.setRecordId(recordId);
                  blurRecord.setColumns(columns);
                  result.add(new BlurMutate(MUTATE_TYPE.REPLACE, blurRecord));
                }
                return new Iterable<Tuple2<Text, BlurMutate>>() {
                  @Override
                  public Iterator<Tuple2<Text, BlurMutate>> iterator() {
                    final Iterator<BlurMutate> iterator = result.iterator();
                    return new Iterator<Tuple2<Text, BlurMutate>>() {

                      @Override
                      public boolean hasNext() {
                        return iterator.hasNext();
                      }

                      @Override
                      public Tuple2<Text, BlurMutate> next() {
                        BlurMutate blurMutate = iterator.next();
                        return new Tuple2<Text, BlurMutate>(new Text(blurMutate.getRecord().getRowId()), blurMutate);
                      }

                      @Override
                      public void remove() {

                      }
                    };
                  }
                };
              }

              private List<BlurColumn> toColumns(List<Column> columns) {
                List<BlurColumn> cols = new ArrayList<BlurColumn>();
                for (Column column : columns) {
                  cols.add(new BlurColumn(column.getName(), column.getValue()));
                }
                return cols;
              }
            });

        final JavaPairRDD<Text, BlurMutate> pRdd = flatMapToPair.partitionBy(blurSparkPartitioner).persist(
            getStorageLevel());
        Job job = new Job(conf);
        BlurOutputFormat.setupJob(job, tableDescriptor);
        Path path = new Path(getOutputPath());
        FileSystem fileSystem = path.getFileSystem(conf);
        Path qualified = fileSystem.makeQualified(path);
        BlurOutputFormat.setOutputPath(job, qualified);
        setupBlurHadoopConfig(job.getConfiguration());
        // Write the RDD to Blur Table
        if (pRdd.count() > 0) {
          pRdd.saveAsNewAPIHadoopFile(tableDescriptor.getTableUri(), Text.class, BlurMutate.class,
              BlurOutputFormat.class, job.getConfiguration());
          client.loadData(getBlurTableName(), qualified.toString());
        }
        return null;
      }
    };
  }
}
