package com.nearinfinity.blur.mapreduce.lib;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.mapreduce.lib.BlurInputFormat;
import com.nearinfinity.blur.mapreduce.lib.BlurInputSplit;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class BlurInputFormatTest {
  
  private Path indexPath = new Path("./tmp/test-indexes/newapi");
  private int numberOfShards = 13;
  private int rowsPerIndex = 10;

  @Before
  public void setup() throws IOException {
    buildTestIndexes(indexPath,numberOfShards,rowsPerIndex);
  }
  
  public static void buildTestIndexes(Path indexPath, int numberOfShards, int rowsPerIndex) throws IOException {
    Configuration configuration = new Configuration();
    FileSystem fileSystem = indexPath.getFileSystem(configuration);
    fileSystem.delete(indexPath, true);
    for (int i = 0; i < numberOfShards; i++) {
      String shardName = BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i);
      buildIndex(fileSystem,configuration, new Path(indexPath,shardName),rowsPerIndex);
    }
  }

  public static void buildIndex(FileSystem fileSystem, Configuration configuration, Path path, int rowsPerIndex) throws IOException {
    HdfsDirectory directory = new HdfsDirectory(path);
    directory.setLockFactory(NoLockFactory.getNoLockFactory());
    BlurAnalyzer analyzer = new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_35));
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, analyzer);
    IndexWriter indexWriter = new IndexWriter(directory, conf);
    RowIndexWriter writer = new RowIndexWriter(indexWriter, analyzer);
    for (int i = 0; i < rowsPerIndex; i++) {
      writer.add(false, genRow());
    }
    indexWriter.close();
  }

  public static Row genRow() {
    Row row = new Row();
    row.setId(UUID.randomUUID().toString());
    for (int i = 0; i < 10; i++) {
      row.addToRecords(genRecord());
    }
    return row;
  }

  public static Record genRecord() {
    Record record = new Record();
    record.setRecordId(UUID.randomUUID().toString());
    record.setFamily("cf");
    record.addToColumns(new Column("name",UUID.randomUUID().toString()));
    return record;
  }

  @Test
  public void testGetSplits() throws IOException, InterruptedException {
    BlurInputFormat format = new BlurInputFormat();
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    FileInputFormat.addInputPath(job, indexPath);
    JobID jobId = new JobID();
    JobContext context = new JobContext(job.getConfiguration(), jobId);
    List<InputSplit> list = format.getSplits(context);
    for (int i = 0; i < list.size(); i++) {
      BlurInputSplit split = (BlurInputSplit) list.get(i);
      Path path = new Path(indexPath,BlurUtil.getShardName(BlurConstants.SHARD_PREFIX, i));
      FileSystem fileSystem = path.getFileSystem(conf);
      assertEquals(new BlurInputSplit(fileSystem.makeQualified(path), "_0", 0, Integer.MAX_VALUE), split);
    }
  }
  
  @Test
  public void testCreateRecordReader() throws IOException, InterruptedException {
    BlurInputFormat format = new BlurInputFormat();
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    FileInputFormat.addInputPath(job, indexPath);
    JobID jobId = new JobID();
    JobContext context = new JobContext(job.getConfiguration(), jobId);
    List<InputSplit> list = format.getSplits(context);
    for (int i = 0; i < list.size(); i++) {
      BlurInputSplit split = (BlurInputSplit) list.get(i);
      TaskAttemptID taskId = new TaskAttemptID();
      TaskAttemptContext taskContext = new TaskAttemptContext(conf, taskId);
      RecordReader<Text, BlurRecord> reader = format.createRecordReader(split, taskContext);
      while (reader.nextKeyValue()) {
        System.out.println(reader.getProgress() + " " + reader.getCurrentKey() + " " + reader.getCurrentValue());
      }
    }
  }

}
