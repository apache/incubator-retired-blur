package org.apache.blur.mapreduce.lib;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.manager.writer.TransactionRecorder;
import org.apache.blur.mapreduce.BlurColumn;
import org.apache.blur.mapreduce.BlurMutate;
import org.apache.blur.mapreduce.BlurRecord;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;

public class BlurRecordWriter extends RecordWriter<Text, BlurMutate> {

  private static Log LOG = LogFactory.getLog(BlurRecordWriter.class);

  private final Text _prevKey = new Text();
  private final List<Document> _documents = new ArrayList<Document>();
  private final IndexWriter _writer;
  private final BlurAnalyzer _analyzer;
  private final StringBuilder _builder = new StringBuilder();

  public BlurRecordWriter(Configuration configuration, BlurAnalyzer blurAnalyzer, int shardId, String tmpDirName) throws IOException {
    Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
    String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, shardId);
    Path indexPath = new Path(tableOutput, shardName);
    Path newIndex = new Path(indexPath,tmpDirName);
    _analyzer = blurAnalyzer;
    IndexWriterConfig conf = new IndexWriterConfig(LuceneVersionConstant.LUCENE_VERSION, _analyzer);
    Directory dir = new HdfsDirectory(configuration, newIndex);
    dir.setLockFactory(NoLockFactory.getNoLockFactory());
    _writer = new IndexWriter(dir, conf);
  }

  @Override
  public void write(Text key, BlurMutate value) throws IOException, InterruptedException {
    if (!_prevKey.equals(key)) {
      flush();
      _prevKey.set(key);
    }
    add(value);
  }

  private void add(BlurMutate value) {
    BlurRecord blurRecord = value.getRecord();
    Record record = getRecord(blurRecord);
    Document document = TransactionRecorder.convert(blurRecord.getRowId(), record, _builder, _analyzer);
    if (_documents.size() == 0) {
      document.add(new StringField(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE, Store.NO));
    }
    _documents.add(document);
    LOG.error("Needs to use blur analyzer and field converter");
  }

  private Record getRecord(BlurRecord value) {
    Record record = new Record();
    record.setRecordId(value.getRecordId());
    record.setFamily(value.getFamily());
    for (BlurColumn col : value.getColumns()) {
      record.addToColumns(new Column(col.getName(), col.getValue()));
    }
    return record;
  }

  private void flush() throws CorruptIndexException, IOException {
    if (_documents.isEmpty()) {
      return;
    }
    _writer.addDocuments(_documents);
    _documents.clear();
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    flush();
    _writer.close();
  }
}
