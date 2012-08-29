package com.nearinfinity.blur.mapreduce.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.mapreduce.BlurColumn;
import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurRecordWriter extends RecordWriter<Text, BlurRecord> {

  private static Log LOG = LogFactory.getLog(BlurRecordWriter.class);

  private Text prevKey = new Text();
  private List<Document> documents = new ArrayList<Document>();
  private IndexWriter writer;

  public BlurRecordWriter(TaskAttemptContext context) throws IOException {
    Configuration configuration = context.getConfiguration();
    String outputPath = configuration.get("mapred.output.dir");
    int id = context.getTaskAttemptID().getTaskID().getId();
    String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, id);
    Path basePath = new Path(outputPath);
    Path indexPath = new Path(basePath, shardName);

    // @TODO
    Analyzer analyzer = new KeywordAnalyzer();

    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);

    // @TODO setup compressed directory, read compression codec from config,
    // setup progressable dir, setup lock factory
    Directory dir = new HdfsDirectory(indexPath);
    dir.setLockFactory(NoLockFactory.getNoLockFactory());
    writer = new IndexWriter(dir, conf);
  }

  @Override
  public void write(Text key, BlurRecord value) throws IOException, InterruptedException {
    if (!prevKey.equals(key)) {
      flush();
      prevKey.set(key);
    }
    add(value);
  }

  private void add(BlurRecord value) {
    List<BlurColumn> columns = value.getColumns();
    String family = value.getFamily();
    Document document = new Document();
    document.add(new Field(BlurConstants.ROW_ID, value.getRowId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    document.add(new Field(BlurConstants.RECORD_ID, value.getRecordId(), Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    for (BlurColumn column : columns) {
      document.add(convert(family, column));
    }
    documents.add(document);
    LOG.error("Needs to use blur analyzer and field converter");
  }

  private Field convert(String family, BlurColumn column) {
    return new Field(family + "." + column.getName(), column.getValue(), Store.YES, Index.ANALYZED_NO_NORMS);
  }

  private void flush() throws CorruptIndexException, IOException {
    if (documents.isEmpty()) {
      return;
    }
    writer.addDocuments(documents);
    documents.clear();
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    flush();
    writer.close();
  }
}
