/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.utils.BlurConstants.ROW_ID;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockObtainFailedException;

import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.index.WalIndexWriter;
import com.nearinfinity.blur.index.WalIndexWriter.WalInputFactory;
import com.nearinfinity.blur.index.WalIndexWriter.WalOutputFactory;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.RowWalIndexWriter;

public class BlurIndexWriter extends AbstractBlurIndex {

  private static final Log LOG = LogFactory.getLog(BlurIndexWriter.class);

  private WalIndexWriter _writer;
  private RowWalIndexWriter _rowIndexWriter;
  private BlurIndexCommiter _commiter;
  private String _id = UUID.randomUUID().toString();
  private BlurMetrics _blurMetrics;

  public void init() throws IOException {
    IndexWriterConfig conf = initIndexWriterConfig();
    _writer = openWriter(conf);
    _writer.commitAndRollWal();
    _rowIndexWriter = new RowWalIndexWriter(_writer, getAnalyzer());
    _commiter.addWriter(_id, _writer);
    initIndexReader(IndexReader.open(_writer, true));
  }

  private WalIndexWriter openWriter(final IndexWriterConfig conf) throws CorruptIndexException, LockObtainFailedException, IOException {
    ExecutorService service = Executors.newSingleThreadExecutor("Writer-Opener-" + getTable() + "-" + getShard() + "-");
    try {
      ExecutorCompletionService<WalIndexWriter> completionService = new ExecutorCompletionService<WalIndexWriter>(service);
      completionService.submit(new Callable<WalIndexWriter>() {
        @Override
        public WalIndexWriter call() throws Exception {
          try {
            return new WalIndexWriter(getDirectory(), conf, _blurMetrics, new WalOutputFactory() {
              @Override
              public IndexOutput getWalOutput(DirectIODirectory directory, String name) throws IOException {
                return directory.createOutputDirectIO(name);
              }
            }, new WalInputFactory() {
              @Override
              public IndexInput getWalInput(DirectIODirectory directory, String name) throws IOException {
                return directory.openInputDirectIO(name);
              }
            });
          } catch (Exception e) {
            LOG.error("Error trying to open table [{0}] shard [{1}]", e, getTable(), getShard());
            throw e;
          }
        }
      });

      while (isOpen()) {
        Future<WalIndexWriter> future;
        try {
          future = completionService.poll(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        if (future == null) {
          LOG.info("Still trying to open shard for writing table [{0}] shard [{1}]", getTable(), getShard());
        } else {
          try {
            return future.get();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CorruptIndexException) {
              throw (CorruptIndexException) cause;
            } else if (cause instanceof LockObtainFailedException) {
              throw (LockObtainFailedException) cause;
            } else if (cause instanceof IOException) {
              throw (IOException) cause;
            } else {
              throw new RuntimeException(cause);
            }
          }
        }
      }
      throw new IOException("Table [" + getTable() + "] shard [" + getShard() + "] not opened");
    } finally {
      service.shutdownNow();
    }
  }

  @Override
  public void refresh() throws IOException {
    synchronized (_writer) {
      try {
        super.refresh();
      } catch (AlreadyClosedException e) {
        LOG.warn("Writer was already closed, this can happen during closing of a writer.");
      }
    }
  }

  @Override
  public void close() throws IOException {
    super.close(new Callable<Void>() {
      public Void call() throws Exception {
        _commiter.remove(_id);
        _writer.close();
        return null;
      }
    });
    LOG.info("Writer for table [{0}] shard [{1}] closed.",getTable(),getShard());
  }

  @Override
  public boolean replaceRow(boolean wal, Row row) throws IOException {
    synchronized (_writer) {
      _rowIndexWriter.replace(wal, row);
      return true;
    }
  }

  @Override
  public void deleteRow(boolean wal, String rowId) throws IOException {
    synchronized (_writer) {
      _writer.deleteDocuments(wal, new Term(ROW_ID, rowId));
    }
  }

  public void setCommiter(BlurIndexCommiter commiter) {
    _commiter = commiter;
  }

  public void setBlurMetrics(BlurMetrics blurMetrics) {
    _blurMetrics = blurMetrics;
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    _writer.forceMerge(numberOfSegmentsPerShard, false);    
  }
}
