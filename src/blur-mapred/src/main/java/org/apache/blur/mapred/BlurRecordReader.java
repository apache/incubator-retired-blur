package org.apache.blur.mapred;

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

import org.apache.blur.mapreduce.BlurRecord;
import org.apache.blur.mapreduce.lib.BlurInputSplit;
import org.apache.blur.mapreduce.lib.Utils;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.utils.RowDocumentUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;


public abstract class BlurRecordReader implements RecordReader<Text, BlurRecord> {

//  private IndexReader reader;
//  private Directory directory;
//  private int startingDocId;
//  private int endingDocId;
//  private int position;
//
//  public BlurRecordReader(InputSplit split, JobConf job) throws IOException {
//    BlurInputSplit blurSplit = (BlurInputSplit) split;
//    Path path = blurSplit.getIndexPath();
//    String segmentName = blurSplit.getSegmentName();
//    startingDocId = blurSplit.getStartingDocId();
//    endingDocId = blurSplit.getEndingDocId();
//    directory = new HdfsDirectory(path);
//
//    IndexCommit commit = Utils.findLatest(directory);
//    reader = Utils.openSegmentReader(directory, commit, segmentName, Utils.getTermInfosIndexDivisor(job));
//    int maxDoc = reader.maxDoc();
//    if (endingDocId >= maxDoc) {
//      endingDocId = maxDoc - 1;
//    }
//    position = startingDocId - 1;
//  }
//
//  @Override
//  public boolean next(Text key, BlurRecord value) throws IOException {
//    do {
//      position++;
//      if (position > endingDocId) {
//        return false;
//      }
//    } while (reader.isDeleted(position));
//    readDocument(key, value);
//    return true;
//  }
//
//  private void readDocument(Text rowid, BlurRecord record) throws CorruptIndexException, IOException {
//    Document document = reader.document(position);
//    record.reset();
//    rowid.set(RowDocumentUtil.readRecord(document, record));
//  }
//
//  @Override
//  public Text createKey() {
//    return new Text();
//  }
//
//  @Override
//  public BlurRecord createValue() {
//    return new BlurRecord();
//  }
//
//  @Override
//  public long getPos() throws IOException {
//    return position;
//  }
//
//  @Override
//  public void close() throws IOException {
//    reader.close();
//    directory.close();
//  }
//
//  @Override
//  public float getProgress() throws IOException {
//    int total = endingDocId - startingDocId;
//    return (float) position / (float) total;
//  }

}
