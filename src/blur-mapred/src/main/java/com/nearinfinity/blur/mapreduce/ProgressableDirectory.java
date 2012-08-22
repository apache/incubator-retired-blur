package com.nearinfinity.blur.mapreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class ProgressableDirectory extends Directory {

  private Directory _directory;
  private Progressable _progressable;

  public ProgressableDirectory(Directory directory, Progressable progressable) {
    _directory = directory;
    _progressable = progressable;
  }

  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  public void close() throws IOException {
    _directory.close();
  }

  public void copy(Directory to, String src, String dest) throws IOException {
    _directory.copy(to, src, dest);
  }

  private IndexInput wrapInput(String name, IndexInput openInput) {
    return new ProgressableIndexInput(name, openInput, 16384, _progressable);
  }

  private IndexOutput wrapOutput(IndexOutput createOutput) {
    return new ProgressableIndexOutput(createOutput, _progressable);
  }

  public IndexOutput createOutput(String name) throws IOException {
    return wrapOutput(_directory.createOutput(name));
  }

  public void deleteFile(String name) throws IOException {
    _directory.deleteFile(name);
  }

  public boolean equals(Object obj) {
    return _directory.equals(obj);
  }

  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  @SuppressWarnings("deprecation")
  public long fileModified(String name) throws IOException {
    return _directory.fileModified(name);
  }

  public LockFactory getLockFactory() {
    return _directory.getLockFactory();
  }

  public String getLockID() {
    return _directory.getLockID();
  }

  public int hashCode() {
    return _directory.hashCode();
  }

  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  public IndexInput openInput(String name, int bufferSize) throws IOException {
    return wrapInput(name, _directory.openInput(name, bufferSize));
  }

  public IndexInput openInput(String name) throws IOException {
    return wrapInput(name, _directory.openInput(name));
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  @SuppressWarnings("deprecation")
  public void sync(String name) throws IOException {
    _directory.sync(name);
  }

  public String toString() {
    return _directory.toString();
  }

  @SuppressWarnings("deprecation")
  public void touchFile(String name) throws IOException {
    _directory.touchFile(name);
  }

  static class ProgressableIndexOutput extends IndexOutput {

    private Progressable _progressable;
    private IndexOutput _indexOutput;

    public ProgressableIndexOutput(IndexOutput indexOutput, Progressable progressable) {
      _indexOutput = indexOutput;
      _progressable = progressable;
    }

    public void close() throws IOException {
      _indexOutput.close();
      _progressable.progress();
    }

    public void copyBytes(DataInput input, long numBytes) throws IOException {
      _indexOutput.copyBytes(input, numBytes);
      _progressable.progress();
    }

    public void flush() throws IOException {
      _indexOutput.flush();
      _progressable.progress();
    }

    public long getFilePointer() {
      return _indexOutput.getFilePointer();
    }

    public long length() throws IOException {
      return _indexOutput.length();
    }

    public void seek(long pos) throws IOException {
      _indexOutput.seek(pos);
      _progressable.progress();
    }

    public void setLength(long length) throws IOException {
      _indexOutput.setLength(length);
      _progressable.progress();
    }

    public String toString() {
      return _indexOutput.toString();
    }

    public void writeByte(byte b) throws IOException {
      _indexOutput.writeByte(b);
    }

    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      _indexOutput.writeBytes(b, offset, length);
      _progressable.progress();
    }

    public void writeBytes(byte[] b, int length) throws IOException {
      _indexOutput.writeBytes(b, length);
      _progressable.progress();
    }

    @SuppressWarnings("deprecation")
    public void writeChars(char[] s, int start, int length) throws IOException {
      _indexOutput.writeChars(s, start, length);
      _progressable.progress();
    }

    @SuppressWarnings("deprecation")
    public void writeChars(String s, int start, int length) throws IOException {
      _indexOutput.writeChars(s, start, length);
      _progressable.progress();
    }

    public void writeInt(int i) throws IOException {
      _indexOutput.writeInt(i);
    }

    public void writeLong(long i) throws IOException {
      _indexOutput.writeLong(i);
    }

    public void writeString(String s) throws IOException {
      _indexOutput.writeString(s);
    }

    public void writeStringStringMap(Map<String, String> map) throws IOException {
      _indexOutput.writeStringStringMap(map);
    }

  }

  static class ProgressableIndexInput extends BufferedIndexInput {

    private IndexInput _indexInput;
    private final long _length;
    private Progressable _progressable;

    ProgressableIndexInput(String name, IndexInput indexInput, int buffer, Progressable progressable) {
      super(name, buffer);
      _indexInput = indexInput;
      _length = indexInput.length();
      _progressable = progressable;
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      long filePointer = getFilePointer();
      if (filePointer != _indexInput.getFilePointer()) {
        _indexInput.seek(filePointer);
      }
      _indexInput.readBytes(b, offset, length);
      _progressable.progress();
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    public void close() throws IOException {
      _indexInput.close();
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public Object clone() {
      ProgressableIndexInput clone = (ProgressableIndexInput) super.clone();
      clone._indexInput = (IndexInput) _indexInput.clone();
      return clone;
    }
  }
}
