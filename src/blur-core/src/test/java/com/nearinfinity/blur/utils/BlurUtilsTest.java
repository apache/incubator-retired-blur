package com.nearinfinity.blur.utils;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import com.nearinfinity.blur.lucene.LuceneConstant;

public class BlurUtilsTest {
  
  @Test
  public void testHumanizeTime1() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 42 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime2() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(42) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("42 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime3() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime4() {
    long time = TimeUnit.HOURS.toMillis(2) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("2 hours 0 minutes 0 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime5() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(37) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("37 seconds", humanizeTime);
  }
  
  @Test
  public void testHumanizeTime6() {
    long time = TimeUnit.HOURS.toMillis(0) + TimeUnit.MINUTES.toMillis(0) + TimeUnit.SECONDS.toMillis(0) + TimeUnit.MILLISECONDS.toMillis(124);
    String humanizeTime = BlurUtil.humanizeTime(time, TimeUnit.MILLISECONDS);
    assertEquals("0 seconds", humanizeTime);
  }
  
  @Test
  public void testMemoryUsage() throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexReader reader = getReader();
    long memoryUsage = BlurUtil.getMemoryUsage(reader);
    assertEquals(42, memoryUsage);
  }

  private IndexReader getReader() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(LuceneConstant.LUCENE_VERSION, new KeywordAnalyzer());
    IndexWriter writer = new IndexWriter(directory,conf);
    Document doc = new Document();
    doc.add(new Field("a","b",Store.YES,Index.NOT_ANALYZED_NO_NORMS));
    writer.addDocument(doc);
    writer.close();
    return IndexReader.open(directory);
  }

}
