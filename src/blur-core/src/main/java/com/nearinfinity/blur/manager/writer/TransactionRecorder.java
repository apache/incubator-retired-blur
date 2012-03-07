package com.nearinfinity.blur.manager.writer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.record.Utils;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.Row;

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

  private AtomicBoolean running = new AtomicBoolean(true);
  private Path walPath;
  private Configuration configuration;
  private FileSystem fileSystem;
  private FSDataOutputStream outputStream;
  private AtomicLong lastSync = new AtomicLong();
  private long timeBetweenSyncs = TimeUnit.MILLISECONDS.toNanos(10);

  public void init() throws IOException {
    fileSystem = walPath.getFileSystem(configuration);
    if (fileSystem.exists(walPath)) {
      outputStream = fileSystem.append(walPath);
    } else {
      outputStream = fileSystem.create(walPath);
    }
    lastSync.set(System.nanoTime());
  }

  public void close() {
    running.set(true);
  }

  private void writeRow(Row row) throws IOException {
    outputStream.writeByte(TYPE.ROW.value());
    writeString(outputStream, row.id);
    List<Record> records = row.records;
    int size = records.size();
    outputStream.writeInt(size);
    for (int i = 0; i < size; i++) {
      Record record = records.get(i);
      writeRecord(outputStream, record);
    }
  }

  private static void writeRecord(FSDataOutputStream outputStream, Record record) throws IOException {
    writeString(outputStream, record.recordId);
    writeString(outputStream, record.family);
    List<Column> columns = record.columns;
    int size = columns.size();
    outputStream.writeInt(size);
    for (int i = 0; i < size; i++) {
      writeColumn(outputStream, columns.get(i));
    }
  }

  private static void writeColumn(FSDataOutputStream outputStream, Column column) throws IOException {
    writeString(outputStream, column.name);
    writeString(outputStream, column.value);
  }

  private void writeDelete(String deleteRowId) throws IOException {
    outputStream.writeByte(TYPE.DELETE.value());
    writeString(outputStream, deleteRowId);
  }

  private static void writeString(FSDataOutputStream outputStream, String s) throws IOException {
    byte[] bs = s.getBytes();
    Utils.writeVInt(outputStream, bs.length);
    outputStream.write(bs);
  }

  private void sync() throws IOException {
    long now = System.nanoTime();
    if (lastSync.get() + timeBetweenSyncs < now) {
      outputStream.sync();
      lastSync.set(now);
    }
  }

  public void replaceRow(Row row) throws IOException {
    synchronized (running) {
      writeRow(row);
      sync();
    }
  }

  public void deleteRow(String rowId) throws IOException {
    synchronized (running) {
      writeDelete(rowId);
      sync();
    }
  }

  public void setWalPath(Path walPath) {
    this.walPath = walPath;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }
}
