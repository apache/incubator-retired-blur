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
package org.apache.blur.mapreduce.lib.v2;

import java.io.IOException;
import java.util.List;

import org.apache.blur.analysis.FieldManager;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

public class DirectIndexingDriver implements Tool {

  public static class DirectIndexingMapper extends
      Mapper<IntWritable, DocumentWritable, LuceneKeyWritable, NullWritable> {

    private FieldManager _fieldManager;
    private TableContext _tableContext;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      try {
        super.run(context);
      } catch (Throwable t) {
        t.printStackTrace();
        throw new IOException(t);
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(context.getConfiguration());
      _tableContext = TableContext.create(tableDescriptor);
      _fieldManager = _tableContext.getFieldManager();
    }

    @Override
    protected void map(IntWritable key, DocumentWritable value, Context context) throws IOException,
        InterruptedException {
      System.out.println(key);
      int shardId = getShardId();
      int documentId = key.get();
      List<IndexableField> document = value.getDocument();
      for (IndexableField field : document) {
        writeField(shardId, documentId, field, context);
      }
    }

    private void writeField(int shardId, int documentId, IndexableField field, Context context) throws IOException,
        InterruptedException {
      int fieldId = _fieldManager.getFieldId(field.name());
      LUCENE_FIELD_TYPE type = LUCENE_FIELD_TYPE.lookupByClass(field.getClass());
      switch (type) {
      case StringField:
        writeStringField(shardId, documentId, fieldId, (StringField) field, context);
        break;
      default:
        throw new IOException("Type [" + type + "] not supported.");
      }
    }

    private void writeStringField(int shardId, int documentId, int fieldId, StringField field, Context context)
        throws IOException, InterruptedException {
      String value = field.stringValue();
      context.write(new LuceneKeyWritable(shardId, fieldId, new BytesRef(value), documentId), NullWritable.get());
    }

    private int getShardId() {
      // TODO Get Shard Id
      return 0;
    }
  }

  public static class DirectIndexingReducer extends
      Reducer<LuceneKeyWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    protected void reduce(LuceneKeyWritable key, Iterable<NullWritable> values, Context context) throws IOException,
        InterruptedException {
      System.out.println(key);
    }

  }

  private Configuration _conf;

  @Override
  public Configuration getConf() {
    return _conf;
  }

  @Override
  public void setConf(Configuration conf) {
    _conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration configuration = getConf();
    String in = args[0];
    String out = args[1];

    Path outputPath = new Path(out);
    FileSystem fileSystem = outputPath.getFileSystem(configuration);
    outputPath = fileSystem.makeQualified(outputPath);

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setTableUri(outputPath.toString());
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(1);

    BlurOutputFormat.setTableDescriptor(configuration, tableDescriptor);

    Job job = new Job(configuration, "Lucene Direct Indexing");
    job.setJarByClass(DirectIndexingDriver.class);
    job.setMapperClass(DirectIndexingMapper.class);
    job.setReducerClass(DirectIndexingReducer.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setOutputKeyClass(LuceneKeyWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    Path inputPath = new Path(in);

    FileInputFormat.addInputPath(job, inputPath);
    FileStatus[] listStatus = fileSystem.listStatus(inputPath);
    for (FileStatus status : listStatus) {
      System.out.println(status.getPath());
    }

    if (!job.waitForCompletion(true)) {
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DFSAdmin(), args));
  }

}
