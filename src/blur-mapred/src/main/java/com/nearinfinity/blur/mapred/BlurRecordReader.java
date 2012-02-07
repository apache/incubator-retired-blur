package com.nearinfinity.blur.mapred;

import java.io.IOException;

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

import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.mapreduce.lib.BlurInputSplit;
import com.nearinfinity.blur.mapreduce.lib.Utils;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.utils.RowDocumentUtil;

public class BlurRecordReader implements RecordReader<Text, BlurRecord> {

  private IndexReader reader;
  private Directory directory;
  private int startingDocId;
  private int endingDocId;
  private int position;
  
  public BlurRecordReader(InputSplit split, JobConf job) throws IOException {
    BlurInputSplit blurSplit = (BlurInputSplit) split;
    Path path = blurSplit.getIndexPath();
    String segmentName = blurSplit.getSegmentName();
    startingDocId = blurSplit.getStartingDocId();
    endingDocId = blurSplit.getEndingDocId();
    directory = new HdfsDirectory(path);
    
    IndexCommit commit = Utils.findLatest(directory);
    reader = Utils.openSegmentReader(directory, commit, segmentName,Utils.getTermInfosIndexDivisor(job));
    int maxDoc = reader.maxDoc();
    if (endingDocId >= maxDoc) {
      endingDocId = maxDoc - 1;
    }
    position = startingDocId - 1;
  }

  @Override
  public boolean next(Text key, BlurRecord value) throws IOException {
    do {
      position++;
      if (position > endingDocId) {
        return false;
      }
    } while (reader.isDeleted(position));
    readDocument(key, value);
    return true;
  }

  private void readDocument(Text rowid, BlurRecord record) throws CorruptIndexException, IOException {
    Document document = reader.document(position);
    record.reset();
    rowid.set(RowDocumentUtil.readRecord(document, record));
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public BlurRecord createValue() {
    return new BlurRecord();
  }

  @Override
  public long getPos() throws IOException {
    return position;
  }

  @Override
  public void close() throws IOException {
    reader.close();
    directory.close();
  }

  @Override
  public float getProgress() throws IOException {
    int total = endingDocId - startingDocId;
    return (float) position / (float) total;
  }

}
