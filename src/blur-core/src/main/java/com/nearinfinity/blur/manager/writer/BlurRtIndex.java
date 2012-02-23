package com.nearinfinity.blur.manager.writer;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.concurrent.Executors;
import com.nearinfinity.blur.manager.writer.lucene.RTIndex;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.RowWalIndexWriter;

public class BlurRtIndex extends BlurIndex {

  private static class IndexMutation implements Callable<Void> {
    String deleteRowId;
    Row row;
    BlurRtIndex index;

    IndexMutation(Row row, BlurRtIndex index) {
      this.row = row;
      this.deleteRowId = row.id;
      this.index = index;
    }

    IndexMutation(String rowId, BlurRtIndex index) {
      this.deleteRowId = rowId;
      this.index = index;
    }

    @Override
    public Void call() throws Exception {
      if (row == null) {
        index.deleteInternal(deleteRowId);
      } else {
        index.deleteInternal(deleteRowId);
        index.addInternal(row);
      }
      return null;
    }
  }
  
  private static final Field PRIME_DOC_FIELD = new Field(PRIME_DOC, PRIME_DOC_VALUE, Store.NO, Index.NOT_ANALYZED_NO_NORMS);
  private static final Term ROW_ID = new Term(BlurConstants.ROW_ID);
  private AtomicBoolean isClosed = new AtomicBoolean();
  private ExecutorService executorService;
  private RTIndex index;
  private final StringBuilder builder = new StringBuilder();

  // externally set
  private int limit = 1000;
  private BlurAnalyzer analyzer;
  private Directory directory;

  public void init() throws IOException {
    executorService = Executors.newSingleThreadExecutor("shard-updater-");
    index = new RTIndex(directory, analyzer, limit);
  }

  /**
   * This method is not thread safe but is only executed by the single thread
   * executor.
   * 
   * @param row
   * @throws IOException
   */
  void addInternal(Row row) throws IOException {
    index.addDocuments(getDocs(row));
  }

  /**
   * This method is not thread safe but is only executed by the single thread
   * executor.
   * 
   * @param deleteRowId
   * @throws IOException
   */
  void deleteInternal(String deleteRowId) throws IOException {
    index.deleteDocuments(ROW_ID.createTerm(deleteRowId));
  }

  Iterable<Document> getDocs(Row row) {
    final String rowId = row.id;
    final Iterator<Record> iterator = row.records.iterator();
    return new Iterable<Document>() {
      private boolean primeDocSet = false; 
      @Override
      public Iterator<Document> iterator() {
        return new Iterator<Document>() {
          @Override
          public boolean hasNext() {
            return iterator.hasNext();
          }

          @Override
          public Document next() {
            Document document = convert(rowId, iterator.next());
            if (!primeDocSet) {
              document.add(PRIME_DOC_FIELD);
              primeDocSet = true;
            }
            
            return document;
          }

          @Override
          public void remove() {

          }
        };
      }
    };
  }
  
  private Document convert(String rowId, Record record) {
    Document document = new Document();
    document.add(new Field(BlurConstants.ROW_ID, rowId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    document.add(new Field(BlurConstants.RECORD_ID, record.recordId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    RowWalIndexWriter.addColumns(document, analyzer, builder, record.family, record.columns);
    return document;
  }

  @Override
  public boolean replaceRow(boolean wal, Row row) throws IOException {
    submitAndWait(new IndexMutation(row, this));
    return true;
  }

  @Override
  public void deleteRow(boolean wal, String rowId) throws IOException {
    submitAndWait(new IndexMutation(rowId, this));
  }

  private void submitAndWait(IndexMutation indexMutation) throws IOException {
    Future<Void> future = executorService.submit(indexMutation);
    try {
      future.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public IndexReader getIndexReader(boolean forceRefresh) throws IOException {
    return index.getIndexReader();
  }

  @Override
  public void close() throws IOException {
    isClosed.set(true);
    executorService.shutdownNow();
    index.close();
  }

  @Override
  public void refresh() throws IOException {
    // do nothing
  }

  @Override
  public AtomicBoolean isClosed() {
    return isClosed;
  }

  @Override
  public void optimize(int numberOfSegmentsPerShard) throws IOException {
    // do nothing
  }
  
  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    this.analyzer = analyzer;
  }

  public void setDirectory(Directory directory) {
    this.directory = directory;
  }
}
