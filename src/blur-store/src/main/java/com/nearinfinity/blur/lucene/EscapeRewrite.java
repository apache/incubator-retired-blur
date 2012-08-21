package com.nearinfinity.blur.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;

public class EscapeRewrite {

  public static void main(String[] args) throws CorruptIndexException, IOException {
    Directory directory = FSDirectory.open(new File("/Users/amccurry/Documents/workspace/low-lat-lucene-rt/index"));
    AtomicBoolean running = new AtomicBoolean(true);
    IndexReader indexReader = IndexReader.open(directory);
    // IndexReader reader = indexReader;
    IndexReader reader = wrap(indexReader, running);
    Query query = new WildcardQuery(new Term("id", "*0*"));
    // Query query = new TermQuery(new Term("id","0"));
    escapeIn(running, TimeUnit.SECONDS.toMillis(5));
    IndexSearcher searcher = new IndexSearcher(reader);
    long s1 = System.nanoTime();
    Query rewrite = searcher.rewrite(query);
    long e1 = System.nanoTime();

    long s2 = System.nanoTime();
    TopDocs topDocs = searcher.search(rewrite, 10);
    long e2 = System.nanoTime();

    System.out.println((e1 - s1) / 1000000.0 + " " + rewrite);
    System.out.println((e2 - s2) / 1000000.0 + " " + topDocs.totalHits);
  }

  private static void escapeIn(final AtomicBoolean running, final long millis) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        running.set(false);
      }
    }).start();
  }

  public static IndexReader wrap(IndexReader reader, AtomicBoolean running) {
    return new IndexReaderRewriteEscape(reader, running);
  }

  public static class IndexReaderRewriteEscape extends IndexReader {
    private IndexReader reader;
    private AtomicBoolean running;

    public IndexReaderRewriteEscape(IndexReader reader, AtomicBoolean running) {
      this.reader = reader;
      this.running = running;
    }

    public String toString() {
      return reader.toString();
    }

    public IndexReader reopen() throws CorruptIndexException, IOException {
      return reader.reopen();
    }

    public IndexReader reopen(boolean openReadOnly) throws CorruptIndexException, IOException {
      return reader.reopen(openReadOnly);
    }

    public IndexReader reopen(IndexCommit commit) throws CorruptIndexException, IOException {
      return reader.reopen(commit);
    }

    public IndexReader reopen(IndexWriter writer, boolean applyAllDeletes) throws CorruptIndexException, IOException {
      return reader.reopen(writer, applyAllDeletes);
    }

    public Directory directory() {
      return reader.directory();
    }

    public long getVersion() {
      return reader.getVersion();
    }

    public boolean isOptimized() {
      return reader.isOptimized();
    }

    public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
      return reader.getTermFreqVectors(docNumber);
    }

    public TermFreqVector getTermFreqVector(int docNumber, String field) throws IOException {
      return reader.getTermFreqVector(docNumber, field);
    }

    public void getTermFreqVector(int docNumber, String field, TermVectorMapper mapper) throws IOException {
      reader.getTermFreqVector(docNumber, field, mapper);
    }

    public void getTermFreqVector(int docNumber, TermVectorMapper mapper) throws IOException {
      reader.getTermFreqVector(docNumber, mapper);
    }

    public int numDocs() {
      return reader.numDocs();
    }

    public int maxDoc() {
      return reader.maxDoc();
    }

    public Document document(int n, FieldSelector fieldSelector) throws CorruptIndexException, IOException {
      return reader.document(n, fieldSelector);
    }

    public boolean hasDeletions() {
      return reader.hasDeletions();
    }

    public boolean hasNorms(String field) throws IOException {
      return reader.hasNorms(field);
    }

    public int docFreq(Term t) throws IOException {
      return reader.docFreq(t);
    }

    public boolean equals(Object arg0) {
      return reader.equals(arg0);
    }

    public Map<String, String> getCommitUserData() {
      return reader.getCommitUserData();
    }

    public FieldInfos getFieldInfos() {
      return reader.getFieldInfos();
    }

    public IndexCommit getIndexCommit() throws IOException {
      return reader.getIndexCommit();
    }

    public Object getCoreCacheKey() {
      return reader.getCoreCacheKey();
    }

    public Object getDeletesCacheKey() {
      return reader.getDeletesCacheKey();
    }

    public long getUniqueTermCount() throws IOException {
      return reader.getUniqueTermCount();
    }

    public int getTermInfosIndexDivisor() {
      return reader.getTermInfosIndexDivisor();
    }

    public int hashCode() {
      return reader.hashCode();
    }

    public boolean isCurrent() throws CorruptIndexException, IOException {
      return reader.isCurrent();
    }

    public boolean isDeleted(int n) {
      return reader.isDeleted(n);
    }

    public byte[] norms(String field) throws IOException {
      return reader.norms(field);
    }

    public void norms(String field, byte[] bytes, int offset) throws IOException {
      reader.norms(field, bytes, offset);
    }

    public TermDocs termDocs(Term term) throws IOException {
      return reader.termDocs(term);
    }

    public TermDocs termDocs() throws IOException {
      return reader.termDocs();
    }

    public TermPositions termPositions() throws IOException {
      return reader.termPositions();
    }

    public Object clone() {
      IndexReaderRewriteEscape clone = (IndexReaderRewriteEscape) super.clone();
      clone.reader = (IndexReader) reader.clone();
      return clone;
    }

    public IndexReader clone(boolean openReadOnly) throws CorruptIndexException, IOException {
      IndexReaderRewriteEscape clone = (IndexReaderRewriteEscape) super.clone();
      clone.reader = reader.clone(openReadOnly);
      return clone;
    }

    public IndexReader[] getSequentialSubReaders() {
      return wrap(reader.getSequentialSubReaders(), running);
    }

    public TermEnum terms() throws IOException {
      return wrap(reader.terms(), running);
    }

    public TermEnum terms(Term t) throws IOException {
      return wrap(reader.terms(t), running);
    }

    @Override
    protected void doSetNorm(int doc, String field, byte value) throws CorruptIndexException, IOException {
      reader.setNorm(doc, field, value);
    }

    @Override
    protected void doDelete(int docNum) throws CorruptIndexException, IOException {
      reader.deleteDocument(docNum);
    }

    @Override
    protected void doUndeleteAll() throws CorruptIndexException, IOException {
      reader.undeleteAll();
    }

    @Override
    protected void doCommit(Map<String, String> commitUserData) throws IOException {
      reader.commit(commitUserData);
    }

    @Override
    protected void doClose() throws IOException {
      reader.close();
    }
  }

  public static TermEnum wrap(final TermEnum terms, final AtomicBoolean running) {
    return new TermEnum() {

      private int count = 0;
      private boolean quit = false;

      @Override
      public Term term() {
        Term term = terms.term();
        System.out.println(term);
        return term;
      }

      @Override
      public boolean next() throws IOException {
        if (quit) {
          return false;
        }
        if (count >= 10000) {
          if (!running.get()) {
            quit = true;
          }
          count = 0;
        }
        count++;
        return terms.next();
      }

      @Override
      public int docFreq() {
        return terms.docFreq();
      }

      @Override
      public void close() throws IOException {
        terms.close();
      }
    };
  }

  public static IndexReader[] wrap(IndexReader[] sequentialSubReaders, AtomicBoolean running) {
    if (sequentialSubReaders == null) {
      return null;
    }
    IndexReader[] result = new IndexReader[sequentialSubReaders.length];
    for (int i = 0; i < sequentialSubReaders.length; i++) {
      result[i] = wrap(sequentialSubReaders[i], running);
    }
    return result;
  }

}
