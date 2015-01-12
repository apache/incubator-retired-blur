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
package org.apache.blur.spark.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.blur.spark.BlurMRBulkLoadSparkProcessor;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.RecordMutation;
import org.apache.blur.thrift.generated.RecordMutationType;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

@SuppressWarnings("serial")
public class SimpleSparkLoaderExample extends BlurMRBulkLoadSparkProcessor<String> {

  public static void main(String[] args) throws IOException {
    SimpleSparkLoaderExample loader = new SimpleSparkLoaderExample();
    loader.setConnectionStr("127.0.0.1:40010");
    // loader.setHdfsDirToMonitor("hdfs://localhost:9000/tmp/spark/input/");
    loader.setHdfsDirToMonitor("file:///tmp/spark-input/");
    loader.setOutputPath("hdfs://localhost:9000/tmp/spark/output-" + System.currentTimeMillis());
    loader.setSparkMaster("spark://amccurry:7077");
    loader.setTableName("test_hdfs");
    loader.run();
  }

  private String _tableName;
  private String _connectionStr;
  private String _hdfsDirToMonitor;
  private String _sparkMaster;
  private String _outputPath;

  @Override
  protected void setupSparkConf(SparkConf conf) {
    conf.set("spark.master", _sparkMaster);
  }

  @Override
  protected String getBlurTableName() {
    return _tableName;
  }

  @Override
  protected Iface getBlurClient() {
    return BlurClient.getClient(_connectionStr);
  }

  @Override
  protected String getAppName() {
    return "Sample Blur Loader";
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<JavaDStream<String>> getStreamsList(JavaStreamingContext ssc) {
    return Arrays.asList(ssc.textFileStream(_hdfsDirToMonitor));
  }

  @Override
  protected RowMutation convert(String s) {
    s = s.trim();
    String rowId = s;
    String recordId = s;
    String value = s;

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("col", value));
    Record record = new Record(recordId, "spark-test", columns);

    RowMutation rowMutation = new RowMutation();
    rowMutation.setTable(getTableName());
    rowMutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    rowMutation.setRowId(rowId);
    rowMutation.addToRecordMutations(new RecordMutation(RecordMutationType.REPLACE_ENTIRE_RECORD, record));
    return rowMutation;
  }

  @Override
  protected String getOutputPath() {
    return _outputPath;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getConnectionStr() {
    return _connectionStr;
  }

  public String getHdfsDirToMonitor() {
    return _hdfsDirToMonitor;
  }

  public String getSparkMaster() {
    return _sparkMaster;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public void setConnectionStr(String connectionStr) {
    _connectionStr = connectionStr;
  }

  public void setHdfsDirToMonitor(String hdfsDirToMonitor) {
    _hdfsDirToMonitor = hdfsDirToMonitor;
  }

  public void setSparkMaster(String sparkMaster) {
    _sparkMaster = sparkMaster;
  }

  public void setOutputPath(String outputPath) {
    _outputPath = outputPath;
  }
}
