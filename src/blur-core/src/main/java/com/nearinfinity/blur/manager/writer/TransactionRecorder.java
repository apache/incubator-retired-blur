package com.nearinfinity.blur.manager.writer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.record.Utils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.NRTManager.TrackingIndexWriter;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.index.IndexWriter;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.RowIndexWriter;

public class TransactionRecorder {

  enum TYPE {
    DELETE((byte) 0), ROW((byte) 1);
    private byte b;

    private TYPE(byte b) {
      this.b = b;
    }

    public byte value() {
      return b;
    }

    public static TYPE lookup(byte b) {
      switch (b) {
      case 0:
        return DELETE;
      case 1:
        return ROW;
      default:
        throw new RuntimeException("Type not found [" + b + "]");
      }
    }
  }

  private static final Log LOG = LogFactory.getLog(TransactionRecorder.class);
  private static final Term ROW_ID = new Term(BlurConstants.ROW_ID);
  private AtomicBoolean running = new AtomicBoolean(true);
  private Path walPath;
  private Configuration configuration;
  private FileSystem fileSystem;
  private AtomicReference<FSDataOutputStream> outputStream = new AtomicReference<FSDataOutputStream>();
  private AtomicLong lastSync = new AtomicLong();
  private long timeBetweenSyncs = TimeUnit.MILLISECONDS.toNanos(10);
  private BlurAnalyzer analyzer;

  public void init() throws IOException {
    fileSystem = walPath.getFileSystem(configuration);
  }

  public void open() throws IOException {
    if (fileSystem.exists(walPath)) {
      throw new IOException("WAL path [" + walPath + "] still exists, replay must have not worked.");
    } else {
      outputStream.set(fileSystem.create(walPath));
    }
    if (outputStream == null) {
      throw new RuntimeException();
    }
    lastSync.set(System.nanoTime());
  }

  public void replay(IndexWriter writer) throws IOException {
    if (fileSystem.exists(walPath)) {
      FSDataInputStream inputStream = fileSystem.open(walPath);
      replay(writer, inputStream);
      inputStream.close();
      commit(writer);
    } else {
      open();
    }
  }

  private void replay(IndexWriter writer, DataInputStream inputStream) throws CorruptIndexException, IOException {
    long updateCount = 0;
    long deleteCount = 0;
    byte[] buffer;
    while ((buffer = readBuffer(inputStream)) != null) {
      DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(buffer));
      TYPE lookup = TYPE.lookup(dataInputStream.readByte());
      switch (lookup) {
      case ROW:
        Row row = readRow(dataInputStream);
        writer.updateDocuments(ROW_ID.createTerm(row.id), getDocs(row, analyzer));
        updateCount++;
        continue;
      case DELETE:
        String deleteRowId = readString(dataInputStream);
        writer.deleteDocuments(ROW_ID.createTerm(deleteRowId));
        deleteCount++;
        continue;
      default:
        LOG.error("Unknown type [{0}]", lookup);
        throw new IOException("Unknown type [" + lookup + "]");
      }
    }
    LOG.info("Rows reclaimed form the WAL [{0}]", updateCount);
    LOG.info("Deletes reclaimed form the WAL [{0}]", deleteCount);
  }

  private byte[] readBuffer(DataInputStream inputStream) {
    try {
      int length = inputStream.readInt();
      byte[] buffer = new byte[length];
      inputStream.readFully(buffer);
      return buffer;
    } catch (IOException e) {
      if (e instanceof EOFException) {
        return null;
      }
      e.printStackTrace();
    }
    return null;
  }

  private void rollLog() throws IOException {
    LOG.info("Rolling WAL path [" + walPath + "]");
    FSDataOutputStream os = outputStream.get();
    if (os != null) {
      os.close();
    }
    fileSystem.delete(walPath, false);
    open();
  }

  public void close() throws IOException {
    synchronized (running) {
      running.set(false);
    }
    outputStream.get().close();
  }

  private static void writeRow(DataOutputStream outputStream, Row row) throws IOException {
    writeString(outputStream, row.id);
    List<Record> records = row.records;
    int size = records.size();
    outputStream.writeInt(size);
    for (int i = 0; i < size; i++) {
      Record record = records.get(i);
      writeRecord(outputStream, record);
    }
  }

  private static Row readRow(DataInputStream inputStream) throws IOException {
    Row row = new Row();
    row.id = readString(inputStream);
    int size = inputStream.readInt();
    for (int i = 0; i < size; i++) {
      row.addToRecords(readRecord(inputStream));
    }
    return row;
  }

  private static void writeRecord(DataOutputStream outputStream, Record record) throws IOException {
    writeString(outputStream, record.recordId);
    writeString(outputStream, record.family);
    List<Column> columns = record.columns;
    int size = columns.size();
    outputStream.writeInt(size);
    for (int i = 0; i < size; i++) {
      writeColumn(outputStream, columns.get(i));
    }
  }

  private static Record readRecord(DataInputStream inputStream) throws IOException {
    Record record = new Record();
    record.recordId = readString(inputStream);
    record.family = readString(inputStream);
    int size = inputStream.readInt();
    for (int i = 0; i < size; i++) {
      record.addToColumns(readColumn(inputStream));
    }
    return record;
  }

  private static void writeColumn(DataOutputStream outputStream, Column column) throws IOException {
    writeString(outputStream, column.name);
    writeString(outputStream, column.value);
  }

  private static Column readColumn(DataInputStream inputStream) throws IOException {
    Column column = new Column();
    column.name = readString(inputStream);
    column.value = readString(inputStream);
    return column;
  }

  private static void writeDelete(DataOutputStream outputStream, String deleteRowId) throws IOException {
    writeString(outputStream, deleteRowId);
  }

  private static void writeString(DataOutputStream outputStream, String s) throws IOException {
    byte[] bs = s.getBytes();
    Utils.writeVInt(outputStream, bs.length);
    outputStream.write(bs);
  }

  private static String readString(DataInputStream inputStream) throws IOException {
    int length = Utils.readVInt(inputStream);
    byte[] buffer = new byte[length];
    inputStream.readFully(buffer);
    return new String(buffer);
  }

  private void sync(byte[] bs) throws IOException {
    if (bs == null || outputStream == null) {
      throw new RuntimeException("bs [" + bs + "] outputStream [" + outputStream + "]");
    }
    FSDataOutputStream os = outputStream.get();
    os.writeInt(bs.length);
    os.write(bs);
    long now = System.nanoTime();
    if (lastSync.get() + timeBetweenSyncs < now) {
      os.sync();
      lastSync.set(now);
    }
  }

  public long replaceRow(boolean wal, Row row, TrackingIndexWriter writer) throws IOException {
    if (wal) {
      synchronized (running) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(baos);
        outputStream.writeByte(TYPE.ROW.value());
        writeRow(outputStream, row);
        outputStream.close();
        sync(baos.toByteArray());
      }
    }
    Term term = ROW_ID.createTerm(row.id);
    List<Document> docs = getDocs(row, analyzer);
    return writer.updateDocuments(term, docs);
  }

  public long deleteRow(boolean wal, String rowId, TrackingIndexWriter writer) throws IOException {
    if (wal) {
      synchronized (running) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(baos);
        outputStream.writeByte(TYPE.DELETE.value());
        writeDelete(outputStream, rowId);
        outputStream.close();
        sync(baos.toByteArray());
      }
    }
    return writer.deleteDocuments(ROW_ID.createTerm(rowId));
  }

  public void setWalPath(Path walPath) {
    this.walPath = walPath;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  public void commit(IndexWriter writer) throws CorruptIndexException, IOException {
    synchronized (running) {
      long s = System.nanoTime();
      writer.commit();
      long m = System.nanoTime();
      LOG.info("Commit took [{0}] for [{1}]", (m - s) / 1000000.0, writer);
      rollLog();
      long e = System.nanoTime();
      LOG.info("Log roller took [{0}] for [{1}]", (e - m) / 1000000.0, writer);
    }
  }

  public static List<Document> getDocs(Row row, BlurAnalyzer analyzer) {
    List<Record> records = row.records;
    int size = records.size();
    final String rowId = row.id;
    final StringBuilder builder = new StringBuilder();
    List<Document> docs = new ArrayList<Document>(size);
    for (int i = 0; i < size; i++) {
      Document document = convert(rowId, records.get(i), builder, analyzer);
      if (i == 0) {
        document.add(BlurConstants.PRIME_DOC_FIELD);
      }
      docs.add(document);
    }
    return docs;
  }

  public static Document convert(String rowId, Record record, StringBuilder builder, BlurAnalyzer analyzer) {
    Document document = new Document();
    document.add(new Field(BlurConstants.ROW_ID, rowId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    document.add(new Field(BlurConstants.RECORD_ID, record.recordId, Store.YES, Index.NOT_ANALYZED_NO_NORMS));
    RowIndexWriter.addColumns(document, analyzer, builder, record.family, record.columns);
    return document;
  }

  public void setAnalyzer(BlurAnalyzer analyzer) {
    this.analyzer = analyzer;
  }
}
