package com.nearinfinity.blur.manager.writer.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.instantiated.InstantiatedIndex;
import org.apache.lucene.store.instantiated.InstantiatedIndexReader;
import org.apache.lucene.store.instantiated.InstantiatedIndexWriter;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class RTIndex {

  private static final Log LOG = LogFactory.getLog(RTIndex.class);

  static class InstantiatedIndexContainer {
    final InstantiatedIndex index;
    final InstantiatedIndexReader reader;
    final InstantiatedIndexWriter writer;
    final IndexReader baseReader;
    Set<Term> deletions;
    int count = 0;
    int maxIndexDocs = 0;

    InstantiatedIndexContainer(IndexReader baseReader) throws IOException {
      this.baseReader = baseReader;
      this.index = new InstantiatedIndex();
      this.count = 0;
      this.writer = index.indexWriterFactory(null, true);
      this.reader = index.indexReaderFactory();
      this.deletions = new HashSet<Term>();
    }

    void commit() throws IOException {
      Set<Term> unflushedDeletions = new HashSet<Term>(writer.getUnflushedDeletions());
      writer.commit();
      unflushedDeletions.addAll(deletions);
      deletions = unflushedDeletions;
      maxIndexDocs = reader.maxDoc();
    }

    void deleteDocuments(Term deleteTerm) throws IOException {
      writer.deleteDocuments(deleteTerm);
      count++;
    }

    void addDocument(Document document, Analyzer analyzer) throws IOException {
      writer.addDocument(document, analyzer);
      count++;
    }
  }

  private final Directory directory;
  private final Analyzer analyzer;
  private final int limit;
  private AtomicReference<InstantiatedIndexContainer> container = new AtomicReference<RTIndex.InstantiatedIndexContainer>();

  public RTIndex(Directory directory, Analyzer analyzer, int limit) throws IOException {
    this.directory = directory;
    this.analyzer = analyzer;
    this.limit = limit;
    initDirectory(directory);
    container.set(new InstantiatedIndexContainer(IndexReader.open(directory)));
  }

  private static void initDirectory(Directory directory) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    new IndexWriter(directory, conf).close();
  }

  public IndexReader getIndexReader() throws IOException {
    InstantiatedIndexContainer indexContainer = container.get();
    IndexReader reader = snapshot(indexContainer.reader, indexContainer.maxIndexDocs);
    IndexReader baseReader = SoftDeleteIndexReader.wrap(indexContainer.baseReader, indexContainer.deletions);
    return new MultiReader(new IndexReader[] { baseReader, reader }, false);
  }

  public void deleteDocuments(Term deleteTerm) throws IOException {
    InstantiatedIndexContainer indexContainer = container.get();
    if (needsFlushBecauseOfDeletes(indexContainer, deleteTerm)) {
      roll();
      indexContainer = container.get();
    }
    indexContainer.deleteDocuments(deleteTerm);
    indexContainer.commit();
    rollIfNeeded();
  }

  private boolean needsFlushBecauseOfDeletes(InstantiatedIndexContainer indexContainer, Term deleteTerm) {
    if (indexContainer.writer.getUnflushedDeletions().contains(deleteTerm)) {
      return true;
    }
    return false;
  }

  public void addDocuments(Iterable<Document> docs) throws IOException {
    InstantiatedIndexContainer indexContainer = container.get();
    for (Document document : docs) {
      indexContainer.addDocument(document, analyzer);
    }
    indexContainer.commit();
    rollIfNeeded();
  }

  public void close() throws IOException {
    closeOldReader(container.get().baseReader);
  }

  private void rollIfNeeded() throws IOException {
    InstantiatedIndexContainer indexContainer = container.get();
    if (indexContainer.count < limit) {
      return;
    }
    roll();
  }

  private void roll() throws IOException {
    InstantiatedIndexContainer indexContainer = container.get();
    container.set(write(indexContainer));
  }

  private InstantiatedIndexContainer write(InstantiatedIndexContainer indexContainer) throws IOException {
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter indexWriter = new IndexWriter(directory, conf);
    indexWriter.deleteDocuments(indexContainer.deletions.toArray(new Term[]{}));
    indexWriter.addIndexes(indexContainer.reader);
    indexWriter.maybeMerge();
    indexWriter.close(true);
    IndexReader newReader = IndexReader.openIfChanged(indexContainer.baseReader, true);
    if (newReader != null) {
      return migrateToNewReader(indexContainer, newReader);
    } else {
      LOG.error("Something strange happened, flushed changes but change in reader.");
      return new InstantiatedIndexContainer(indexContainer.baseReader);
    }
  }

  private InstantiatedIndexContainer migrateToNewReader(InstantiatedIndexContainer indexContainer, IndexReader newReader) throws IOException {
    closeOldReader(indexContainer.baseReader);
    return new InstantiatedIndexContainer(newReader);
  }

  private void closeOldReader(IndexReader oldReader) {
    // TODO Auto-generated method stub

  }

  private IndexReader snapshot(IndexReader reader, int maxDocs) throws IOException {
    return SnapshotIndexReader.wrap(reader, maxDocs);
  }
}
