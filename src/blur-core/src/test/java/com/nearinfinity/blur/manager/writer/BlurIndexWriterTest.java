package com.nearinfinity.blur.manager.writer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.DirectIODirectory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;

public class BlurIndexWriterTest {

  private static final int TEST_NUMBER = 10000;
  private BlurIndexWriter writer;
  private BlurIndexCloser closer;
  private Random random = new Random();
  private BlurIndexRefresher refresher;
  private File dir;
  private BlurIndexCommiter commiter;

  @Before
  public void setup() throws IOException {
    dir = new File("./tmp/blur-index-writer-test");
    rm(dir);
    dir.mkdirs();
    closer = new BlurIndexCloser();
    closer.init();

    BlurAnalyzer analyzer = new BlurAnalyzer(new KeywordAnalyzer());

    refresher = new BlurIndexRefresher();
    refresher.init();

    commiter = new BlurIndexCommiter();
    commiter.init();

    writer = new BlurIndexWriter();
    writer.setDirectory(DirectIODirectory.wrap(FSDirectory.open(dir)));
    writer.setCloser(closer);
    writer.setAnalyzer(analyzer);
    writer.setRefresher(refresher);
    writer.setCommiter(commiter);
    writer.setSimilarity(new FairSimilarity());
    writer.setBlurMetrics(new BlurMetrics(new Configuration()));
    writer.init();
  }

  @After
  public void tearDown() throws IOException {
    commiter.close();
    refresher.close();
    writer.close();
    closer.close();
    rm(dir);
  }

  private void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

  @Test
  public void testBlurIndexWriter() throws IOException {
    long s = System.nanoTime();
    int total = 0;
    for (int i = 0; i < TEST_NUMBER; i++) {
      writer.replaceRow(true, genRow());
      total++;
    }
    long e = System.nanoTime();
    double seconds = (e - s) / 1000000000.0;
    double rate = total / seconds;
    System.out.println("Rate " + rate);
    IndexReader reader = writer.getIndexReader(true);
    assertEquals(TEST_NUMBER, reader.numDocs());
  }

  private Row genRow() {
    Row row = new Row();
    row.setId(Long.toString(random.nextLong()));
    Record record = new Record();
    record.setFamily("testing");
    record.setRecordId(Long.toString(random.nextLong()));
    for (int i = 0; i < 10; i++) {
      record.addToColumns(new Column("col" + i, Long.toString(random.nextLong())));
    }
    row.addToRecords(record);
    return row;
  }

}
