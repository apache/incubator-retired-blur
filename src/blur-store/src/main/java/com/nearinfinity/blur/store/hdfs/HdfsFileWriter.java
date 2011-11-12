package com.nearinfinity.blur.store.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.DataOutput;

public class HdfsFileWriter extends DataOutput {

  public static final int VERSION = -1;

  private FSDataOutputStream _outputStream;
  private HdfsMetaBlock _block;
  private List<HdfsMetaBlock> _blocks = new ArrayList<HdfsMetaBlock>();
  private long _length;
  private long _currentPosition;
  
  public HdfsFileWriter(FileSystem fileSystem, Path path) throws IOException {
    _outputStream = fileSystem.create(path);
    seek(0);
  }
  
  public long length() {
    return _length;
  }

  public void seek(long pos) throws IOException {
    if (_block != null) {
      _blocks.add(_block);
    }
    _block = new HdfsMetaBlock();
    _block.realPosition = _outputStream.getPos();
    _block.logicalPosition = pos;
    _currentPosition = pos;
  }

  public void close() throws IOException {
    if (_block != null) {
      _blocks.add(_block);
    }
    flushMetaBlocks();
    _outputStream.close();
  }

  private void flushMetaBlocks() throws IOException {
    for (HdfsMetaBlock block : _blocks) {
      block.write(_outputStream);
    }
    _outputStream.writeInt(_blocks.size());
    _outputStream.writeLong(length());
    _outputStream.writeInt(VERSION);
  }

  @Override
  public void writeByte(byte b) throws IOException {
    _outputStream.write(b & 0xFF);
    _block.length++;
    _currentPosition++;
    updateLength();
  }

  @Override
  public void writeBytes(byte[] b, int offset, int length) throws IOException {
    _outputStream.write(b, offset, length);
    _block.length+=length;
    _currentPosition+=length;
    updateLength();
  }

  private void updateLength() {
    if (_currentPosition > _length) {
      _length = _currentPosition;
    }
  }

  public long getPosition() {
    return _currentPosition;
  }
}
