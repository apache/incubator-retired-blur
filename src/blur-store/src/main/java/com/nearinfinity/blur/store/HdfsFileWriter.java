package com.nearinfinity.blur.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.DataOutput;

public class HdfsFileWriter extends DataOutput {

  private static final int VERSION = -1;

  public static void main(String[] args) throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path p = new Path("file:///tmp/testint.hdfs.writer");
    
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    
    HdfsFileWriter hdfsFile = new HdfsFileWriter(fs,p);
    Random random = new Random(1);
    for (int i = 0; i < 50000; i++) {
      hdfsFile.writeInt(i);
    }
    for (int i = 0; i < 10; i++) {
      int pos = random.nextInt(50000) * 4;
      hdfsFile.seek(pos);
      System.out.println("pos=" + pos);
      hdfsFile.writeInt(Integer.MAX_VALUE);
    }
    hdfsFile.close();
    
    System.out.println(hdfsFile.length());
  }

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
