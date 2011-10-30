package com.nearinfinity.blur.store;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

public class NullIndexOutput extends IndexOutput {

  private long _pos;
  private long _length;

  @Override
  public void close() throws IOException {

  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public long getFilePointer() {
    return _pos;
  }

  @Override
  public long length() throws IOException {
    return _length;
  }

  @Override
  public void seek(long pos) throws IOException {
    _pos = pos;
  }

  @Override
  public void writeByte(byte b) throws IOException {
    _pos++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    _pos += length;
    updateLength();
  }

  private void updateLength() {
    if (_pos > _length) {
      _length = _pos;
    }
  }

}
