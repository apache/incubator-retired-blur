package com.nearinfinity.blur.index;

import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.metrics.BlurMetrics;

public class WalIndexWriterTest {

  private DirectIODirectory directory;
  
  public interface Action {
    void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException;
  }

  @Before
  public void setUp() throws IOException {
    File path = new File("./tmp/wal-index");
    rm(path);
    path.mkdirs();
    Directory dir = FSDirectory.open(path);
    directory = DirectIODirectory.wrap(dir);
    directory.setLockFactory(new SingleInstanceLockFactory());
  }
  
  private WalIndexWriter getIndexWriter() throws CorruptIndexException, LockObtainFailedException, IOException {
    IndexWriterConfig config = new IndexWriterConfig(LUCENE_VERSION, new StandardAnalyzer(LUCENE_VERSION));
    Configuration conf = new Configuration();
    BlurMetrics blurMetrics = new BlurMetrics(conf);
    return new WalIndexWriter(directory, config, blurMetrics);
  }

  @Test
  public void testAddDocumentWalTrue() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Document doc = getDoc();
        indexWriter.addDocument(true, doc);
      }
    },0,1);
  }
  
  @Test
  public void testAddDocumentWalFalse() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Document doc = getDoc();
        indexWriter.addDocument(false, doc);
      }
    },0,0);
  }
  
  @Test
  public void testUpdateDocumentWalTrue() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Document doc = getDoc();
        indexWriter.updateDocument(true, new Term("f","v1"), doc);
      }
    },0,1);
  }
  
  @Test
  public void testUpdateDocumentWalFalse() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Document doc = getDoc();
        indexWriter.updateDocument(false, new Term("f","v1"), doc);
      }
    },0,0);
  }
  
  @Test
  public void testAddDocumentsWalTrue() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Collection<Document> docs = getDocs();
        indexWriter.addDocuments(true, docs);
      }
    },0,2);
  }
  
  @Test
  public void testAddDocumentsWalFalse() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Collection<Document> docs = getDocs();
        indexWriter.addDocuments(false, docs);
      }
    },0,0);
  }
  
  @Test
  public void testUpdateDocumentsWalTrue() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Collection<Document> docs = getDocs();
        indexWriter.updateDocuments(true, new Term("f","v1"), docs);
      }
    },0,2);
  }
  
  @Test
  public void testUpdateDocumentsWalFalse() throws CorruptIndexException, LockObtainFailedException, IOException {
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        Collection<Document> docs = getDocs();
        indexWriter.updateDocuments(false, new Term("f","v1"), docs);
      }
    },0,0);
  }

  @Test
  public void testDeleteDocumentsByTerm() throws CorruptIndexException, LockObtainFailedException, IOException {
    testAddDocumentsWalTrue();
    runTest(new Action() {
      @Override
      public void action(WalIndexWriter indexWriter) throws CorruptIndexException, IOException {
        indexWriter.deleteDocuments(true, new Term("f","v"));
      }
    },2,0);
  }

  // NOTE: Expect this to fail because deletion by query is not implemented
  //       yet.  Previous versions had a problem that would blow up the
  //       stack with an infinite recursion, however, so this verifies that
  //       problem has been fixed and we correctly detect the unsupported
  //       operation.
  @Test(expected=IOException.class)
  public void testDeleteDocumentsByQuery() throws CorruptIndexException, LockObtainFailedException, IOException {
    getIndexWriter().close();
    WalIndexWriter indexWriter = getIndexWriter();
    indexWriter.deleteDocuments(true, new TermQuery(new Term("f","v")));
  }
  
  private Collection<Document> getDocs() {
    List<Document> docs = new ArrayList<Document>();
    docs.add(getDoc());
    docs.add(getDoc());
    return docs;
  }

  private void runTest(Action action, int countBeforeAction, int countAfterAction) throws CorruptIndexException, LockObtainFailedException, IOException {
    getIndexWriter().close();
    WalIndexWriter indexWriter1 = getIndexWriter();
    try {
      action.action(indexWriter1);
      indexWriter1.rollback();
      indexWriter1.close();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    IndexReader reader1 = IndexReader.open(directory);
    assertEquals(countBeforeAction,reader1.numDocs());
    reader1.close();
    WalIndexWriter indexWriter = getIndexWriter();
    indexWriter.commitAndRollWal();
    indexWriter.close();
    
    IndexReader reader2 = IndexReader.open(directory);
    assertEquals(countAfterAction,reader2.numDocs());
    reader2.close();
  }

  private Document getDoc() {
    Document document = new Document();
    document.add(new Field("f","v",Store.YES,Index.ANALYZED_NO_NORMS));
    return document;
  }

  private static void rm(File file) {
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
}
