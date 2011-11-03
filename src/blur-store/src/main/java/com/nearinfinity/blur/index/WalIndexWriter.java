package com.nearinfinity.blur.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;

import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.DirectIODirectory;

public class WalIndexWriter extends IndexWriter {

  private static final String EXT = ".wal";
  private static final String PREFIX = "__";

  private static final Log LOG = LogFactory.getLog(WalIndexWriter.class);
  private static final long MAX_COMMIT_LOG_TIME = TimeUnit.HOURS.toMillis(1);

  private DirectIODirectory _directory;
  private AtomicReference<IndexOutput> _walOutput = new AtomicReference<IndexOutput>();
  private String _currentWalName;
  private WalOutputFactory _walOutputFactory;
  private WalInputFactory _walInputFactory;
  private BlurMetrics _blurMetrics;
  private AtomicLong _dirty = new AtomicLong(0);
  private long _lastCommit = -1;
  private long _lastCommitTime = 0;
  private static ExecutorService _service;

  public static interface WalOutputFactory {
    IndexOutput getWalOutput(DirectIODirectory directory, String name) throws IOException;
  }

  public static interface WalInputFactory {
    IndexInput getWalInput(DirectIODirectory directory, String name) throws IOException;
  }

  public WalIndexWriter(DirectIODirectory directory, IndexWriterConfig config, BlurMetrics blurMetrics) throws CorruptIndexException, LockObtainFailedException, IOException {
    this(directory, config, blurMetrics, new WalOutputFactory() {
      @Override
      public IndexOutput getWalOutput(DirectIODirectory directory, String name) throws IOException {
        return directory.createOutput(name);
      }
    }, new WalInputFactory() {
      @Override
      public IndexInput getWalInput(DirectIODirectory directory, String name) throws IOException {
        return directory.openInput(name);
      }
    });
  }

  public WalIndexWriter(DirectIODirectory directory, IndexWriterConfig config, BlurMetrics blurMetrics, WalOutputFactory walOutputFactory, WalInputFactory walInputFactory)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    super(directory, config);
    _walOutputFactory = walOutputFactory;
    _walInputFactory = walInputFactory;
    _directory = directory;
    _blurMetrics = blurMetrics;
    replayWal();

    long nanoTime = System.nanoTime();
    String name = PREFIX + Long.toHexString(nanoTime) + EXT;
    IndexOutput walOutput = _walOutputFactory.getWalOutput(_directory, name);
    _walOutput.set(walOutput);
    _currentWalName = name;
    _service = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void deleteDocuments(boolean wal, Query... queries) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (wal) {
      Future<Boolean> valid = walWriterDelete(queries);
      super.deleteDocuments(queries);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.deleteDocuments(queries);
    }
  }

  public void deleteDocuments(boolean wal, Term... terms) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (wal) {
      Future<Boolean> valid = walWriterDelete(terms);
      super.deleteDocuments(terms);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.deleteDocuments(terms);
    }
  }

  public void updateDocument(boolean wal, Term term, Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (term == null) {
      addDocument(wal, doc, analyzer);
      return;
    }
    if (wal) {
      Future<Boolean> valid = walWriterUpdate(term, doc);
      super.updateDocument(term, doc, analyzer);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.updateDocument(term, doc, analyzer);
    }
    _blurMetrics.recordWrites.incrementAndGet();
  }

  public void updateDocuments(boolean wal, Term term, Collection<Document> docs, Analyzer analyzer) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (term == null) {
      addDocuments(wal, docs, analyzer);
      return;
    }
    if (wal) {
      Future<Boolean> valid = walWriterUpdate(term, docs);
      super.updateDocuments(term, docs, analyzer);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.updateDocuments(term, docs, analyzer);
    }
    _blurMetrics.recordWrites.incrementAndGet();
  }

  public void deleteDocuments(boolean wal, Query query) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    deleteDocuments(wal, query);
  }

  public void deleteDocuments(boolean wal, Term term) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    deleteDocuments(wal, term);
  }

  public void addDocument(boolean wal, Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (wal) {
      Future<Boolean> valid = walWriterAdd(doc);
      super.addDocument(doc, analyzer);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.addDocument(doc, analyzer);
    }
    _blurMetrics.recordWrites.incrementAndGet();
  }

  public void addDocuments(boolean wal, Collection<Document> docs, Analyzer analyzer) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    if (wal) {
      Future<Boolean> valid = walWriterAdd(docs);
      super.addDocuments(docs, analyzer);
      try {
        if (!valid.get()) {
          throw new IOException("Was not written to WAL");
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    } else {
      super.addDocuments(docs, analyzer);
    }
    _blurMetrics.recordWrites.incrementAndGet();
  }

  @Override
  public void close() throws CorruptIndexException, IOException {
    LOG.info("Starting close");
    super.close();
    LOG.info("Shutting down executor pool");
    _service.shutdown();
    try {
      _service.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      return;
    }
  }

  public void commitAndRollWal() throws CorruptIndexException, IOException {
    long commit = _dirty.get();
    if (commit != _lastCommit || _lastCommitTime + MAX_COMMIT_LOG_TIME < System.currentTimeMillis()) {
      List<String> oldLogs = rollWal();
      commit();
      removeOldWals(oldLogs);
      _lastCommit = commit;
      _lastCommitTime = System.currentTimeMillis();
    } else {
      LOG.info("No commit needed");
    }
  }

  private void replayWal() throws IOException {
    LOG.info("Checking for WAL files.");
    String[] listAll = _directory.listAll();
    List<String> allFiles = new ArrayList<String>(Arrays.asList(listAll));
    Collections.sort(allFiles);
    long total = 0;
    for (String file : allFiles) {
      if (file.endsWith(EXT)) {
        total += replay(file);
      }
    }
    LOG.info("Total docs reclaimed from logs [" + total + "]");
  }

  private void removeOldWals(List<String> oldLogs) throws IOException {
    for (String name : oldLogs) {
      _directory.deleteFile(name);
    }
  }

  private List<String> rollWal() throws IOException {
    LOG.info("Rolling WAL file");
    long nanoTime = System.nanoTime();
    synchronized (_walOutput) {
      IndexOutput output = _walOutput.get();
      LOG.info("Closing WAL [" + _currentWalName + "]");
      output.close();
      String name = PREFIX + Long.toHexString(nanoTime) + EXT;
      IndexOutput walOutput = _walOutputFactory.getWalOutput(_directory, name);
      _walOutput.set(walOutput);
      LOG.info("Open WAL [" + _currentWalName + "]");
      _currentWalName = name;
    }
    String[] listAll = _directory.listAll();
    List<String> files = new ArrayList<String>();
    for (String name : listAll) {
      if (name.endsWith(EXT) && !name.equals(_currentWalName)) {
        files.add(name);
      }
    }
    return files;
  }

  private Future<Boolean> walWriterAdd(final Document doc) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentAdd(output, doc);
          output.flush();
          return true;
        }
      }
    });
  }

  private Future<Boolean> walWriterAdd(final Collection<Document> docs) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentsAdd(output, docs);
          output.flush();
          return true;
        }
      }
    });
  }

  private Future<Boolean> walWriterUpdate(final Term term, final Document doc) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentUpdate(output, term, doc);
          output.flush();
          return true;
        }
      }
    });
  }

  private Future<Boolean> walWriterUpdate(final Term term, final Collection<Document> docs) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentsUpdate(output, term, docs);
          output.flush();
          return true;
        }
      }
    });
  }

  private Future<Boolean> walWriterDelete(final Query... queries) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentsDelete(output, queries);
          output.flush();
          return true;
        }
      }
    });
  }

  private Future<Boolean> walWriterDelete(final Term... terms) {
    return _service.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        synchronized (_walOutput) {
          IndexOutput output = _walOutput.get();
          WalFile.writeActionDocumentsDelete(output, terms);
          output.flush();
          return true;
        }
      }
    });
  }

  private long replay(String name) throws IOException {
    LOG.info("Replaying file [" + name + "]");
    IndexInput input = _walInputFactory.getWalInput(_directory, name);
    try {
      return WalFile.replayInput(input, this);
    } finally {
      input.close();
    }
  }

  public void addDocument(boolean wal, Document doc) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    addDocument(wal, doc, getAnalyzer());
  }

  public void addDocuments(boolean wal, Collection<Document> docs) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    addDocuments(wal, docs, getAnalyzer());
  }

  public void updateDocument(boolean wal, Term term, Document doc) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    updateDocument(wal, term, doc, getAnalyzer());
  }

  public void updateDocuments(boolean wal, Term delTerm, Collection<Document> docs) throws CorruptIndexException, IOException {
    _dirty.incrementAndGet();
    updateDocuments(wal, delTerm, docs, getAnalyzer());
  }
}
