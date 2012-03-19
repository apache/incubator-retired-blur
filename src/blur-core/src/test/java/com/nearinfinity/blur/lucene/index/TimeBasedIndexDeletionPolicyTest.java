package com.nearinfinity.blur.lucene.index;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import com.nearinfinity.blur.index.IndexWriter;

public class TimeBasedIndexDeletionPolicyTest {

  @Test
  public void testTimeBasedIndexDeletionPolicy() throws IOException, InterruptedException {
    TimeBasedIndexDeletionPolicy indexDeletionPolicy = new TimeBasedIndexDeletionPolicy(3000);
    RAMDirectory directory = new RAMDirectory();
    addAndCommit(directory, indexDeletionPolicy);
    addAndCommit(directory, indexDeletionPolicy);
    addAndCommit(directory, indexDeletionPolicy);
    Thread.sleep(1000);
    assertEquals(3, IndexReader.listCommits(directory).size());
    Thread.sleep(4000);
    addAndCommit(directory, indexDeletionPolicy);
    assertEquals(2, IndexReader.listCommits(directory).size());
    Thread.sleep(4000);
    openClose(directory, indexDeletionPolicy);
    assertEquals(1, IndexReader.listCommits(directory).size());
  }

  private void openClose(RAMDirectory directory, TimeBasedIndexDeletionPolicy indexDeletionPolicy) throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    conf.setIndexDeletionPolicy(indexDeletionPolicy);
    new IndexWriter(directory, conf).close();
  }

  private void addAndCommit(RAMDirectory directory, IndexDeletionPolicy indexDeletionPolicy) throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    conf.setIndexDeletionPolicy(indexDeletionPolicy);
    IndexWriter writer = new IndexWriter(directory, conf);
    Document doc = new Document();
    doc.add(new Field("id", "1", Store.YES, Index.ANALYZED_NO_NORMS));
    writer.addDocument(doc);
    writer.close();
  }

}
