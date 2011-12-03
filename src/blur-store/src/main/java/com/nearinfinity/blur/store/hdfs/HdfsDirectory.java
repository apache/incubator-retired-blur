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

package com.nearinfinity.blur.store.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.blur.index.DirectIODirectory;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class HdfsDirectory extends DirectIODirectory {

  private static final String LF_EXT = ".lf";
  protected static final String SEGMENTS_GEN = "segments.gen";
  protected static final IndexOutput NULL_WRITER = new NullIndexOutput();
  protected Path _hdfsDirPath;
  protected AtomicReference<FileSystem> _fileSystemRef = new AtomicReference<FileSystem>();
  protected Configuration _configuration;

  public HdfsDirectory(Path hdfsDirPath) throws IOException {
    _hdfsDirPath = hdfsDirPath;
    _configuration = new Configuration();
    reopenFileSystem();
    try {
      if (!getFileSystem().exists(hdfsDirPath)) {
        getFileSystem().mkdirs(hdfsDirPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public IndexOutput createOutput(String name) throws IOException {
    if (SEGMENTS_GEN.equals(name)) {
      return NULL_WRITER;
    }
    name = getRealName(name);
    HdfsFileWriter writer = new HdfsFileWriter(getFileSystem(), new Path(_hdfsDirPath, name));
    return new HdfsLayeredIndexOutput(writer);
  }

  private String getRealName(String name) throws IOException {
    if (getFileSystem().exists(new Path(_hdfsDirPath, name))) {
      return name;
    }
    return name + LF_EXT;
  }

  private String[] getNormalNames(List<String> files) {
    int size = files.size();
    for (int i = 0; i < size; i++) {
      String str = files.get(i);
      files.set(i, toNormalName(str));
    }
    return files.toArray(new String[] {});
  }

  private String toNormalName(String name) {
    if (name.endsWith(LF_EXT)) {
      return name.substring(0, name.length() - 3);
    }
    return name;
  }

  @Override
  public IndexInput openInput(String name) throws IOException {
    return openInput(name, BUFFER_SIZE);
  }

  @Override
  public IndexInput openInput(String name, int bufferSize) throws IOException {
    name = getRealName(name);
    if (isLayeredFile(name)) {
      HdfsFileReader reader = new HdfsFileReader(getFileSystem(), new Path(_hdfsDirPath, name), BUFFER_SIZE);
      return new HdfsLayeredIndexInput(reader, BUFFER_SIZE);
    } else {
      return new HdfsNormalIndexInput(getFileSystem(), new Path(_hdfsDirPath, name), BUFFER_SIZE);
    }
  }

  private boolean isLayeredFile(String name) {
    if (name.endsWith(LF_EXT)) {
      return true;
    }
    return false;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    name = getRealName(name);
    if (!fileExists(name)) {
      throw new FileNotFoundException(name);
    }
    getFileSystem().delete(new Path(_hdfsDirPath, name), false);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    name = getRealName(name);
    return getFileSystem().exists(new Path(_hdfsDirPath, name));
  }

  @Override
  public long fileLength(String name) throws IOException {
    name = getRealName(name);
    if (!fileExists(name)) {
      throw new FileNotFoundException(name);
    }
    return HdfsFileReader.getLength(getFileSystem(), new Path(_hdfsDirPath, name));
  }

  @Override
  public long fileModified(String name) throws IOException {
    name = getRealName(name);
    if (!fileExists(name)) {
      throw new FileNotFoundException(name);
    }
    FileStatus fileStatus = getFileSystem().getFileStatus(new Path(_hdfsDirPath, name));
    return fileStatus.getModificationTime();
  }

  @Override
  public String[] listAll() throws IOException {
    FileStatus[] listStatus = getFileSystem().listStatus(_hdfsDirPath);
    List<String> files = new ArrayList<String>();
    for (FileStatus status : listStatus) {
      if (!status.isDir()) {
        files.add(status.getPath().getName());
      }
    }
    return getNormalNames(files);
  }

  @Override
  public void touchFile(String name) throws IOException {
    // do nothing
  }

  public Path getHdfsDirPath() {
    return _hdfsDirPath;
  }

  public FileSystem getFileSystem() {
    return _fileSystemRef.get();
  }

  protected void reopenFileSystem() throws IOException {
    FileSystem fileSystem = FileSystem.get(_hdfsDirPath.toUri(), _configuration);
    FileSystem oldFs = _fileSystemRef.get();
    _fileSystemRef.set(fileSystem);
    if (oldFs != null) {
      oldFs.close();
    }
  }

  static class HdfsLayeredIndexInput extends BufferedIndexInput {

    private HdfsFileReader _reader;
    private long _length;
    private boolean isClone;

    public HdfsLayeredIndexInput(HdfsFileReader reader, int bufferSize) {
      super(bufferSize);
      _reader = reader;
      _length = _reader.length();
    }

    @Override
    public void close() throws IOException {
      if (!isClone) {
        _reader.close();
      }
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public Object clone() {
      HdfsLayeredIndexInput input = (HdfsLayeredIndexInput) super.clone();
      input.isClone = true;
      input._reader = (HdfsFileReader) _reader.clone();
      return input;
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      long position = getFilePointer();
      _reader.seek(position);
      _reader.readBytes(b, offset, length);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
      // do nothing
    }
  }

  static class HdfsNormalIndexInput extends BufferedIndexInput {

    private final FSDataInputStream _inputStream;
    private final long _length;
    private boolean _clone = false;

    public HdfsNormalIndexInput(FileSystem fileSystem, Path path, int bufferSize) throws IOException {
      FileStatus fileStatus = fileSystem.getFileStatus(path);
      _length = fileStatus.getLen();
      _inputStream = fileSystem.open(path, bufferSize);
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      _inputStream.read(getFilePointer(), b, offset, length);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    public void close() throws IOException {
      if (!_clone) {
        _inputStream.close();
      }
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public Object clone() {
      HdfsNormalIndexInput clone = (HdfsNormalIndexInput) super.clone();
      clone._clone = true;
      return clone;
    }
  }

  static class HdfsLayeredIndexOutput extends IndexOutput {

    private HdfsFileWriter _writer;

    public HdfsLayeredIndexOutput(HdfsFileWriter writer) {
      _writer = writer;
    }

    @Override
    public void close() throws IOException {
      _writer.close();
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public long getFilePointer() {
      return _writer.getPosition();
    }

    @Override
    public long length() throws IOException {
      return _writer.length();
    }

    @Override
    public void seek(long pos) throws IOException {
      _writer.seek(pos);
    }

    @Override
    public void writeByte(byte b) throws IOException {
      _writer.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      _writer.writeBytes(b, offset, length);
    }
  }

  @Override
  public IndexOutput createOutputDirectIO(String name) throws IOException {
    FileSystem fileSystem = getFileSystem();
    final FSDataOutputStream outputStream = fileSystem.create(new Path(_hdfsDirPath, name));
    return new IndexOutput() {

      @Override
      public void close() throws IOException {
        outputStream.close();
      }

      @Override
      public void flush() throws IOException {
        outputStream.sync();
      }

      @Override
      public long getFilePointer() {
        try {
          return outputStream.getPos();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public long length() throws IOException {
        return outputStream.getPos();
      }

      @Override
      public void seek(long pos) throws IOException {
        throw new IOException("Seeks not allowed");
      }

      @Override
      public void writeByte(byte b) throws IOException {
        outputStream.write(b & 0xFF);
      }

      @Override
      public void writeBytes(byte[] b, int offset, int length) throws IOException {
        outputStream.write(b, offset, length);
      }
    };
  }

  @Override
  public IndexInput openInputDirectIO(String name) throws IOException {
    Path path = new Path(_hdfsDirPath, name);
    FSDataInputStream inputStream = getFileSystem().open(path);
    return new DirectIOHdfsIndexInput(inputStream, realFileLength(path));
  }

  private long realFileLength(Path path) throws IOException {
    FileSystem fileSystem = getFileSystem();
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    return fileStatus.getLen();
  }

  static class DirectIOHdfsIndexInput extends BufferedIndexInput {

    private long _length;
    private FSDataInputStream _inputStream;
    private boolean isClone;

    public DirectIOHdfsIndexInput(FSDataInputStream inputStream, long length) throws IOException {
      if (inputStream instanceof DFSDataInputStream) {
        // This is needed because if the file was in progress of being written
        // but
        // was not closed the
        // length of the file is 0. This will fetch the synced length of the
        // file.
        _length = ((DFSDataInputStream) inputStream).getVisibleLength();
      } else {
        _length = length;
      }
      _inputStream = inputStream;
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public void close() throws IOException {
      if (!isClone) {
        _inputStream.close();
      }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      synchronized (_inputStream) {
        _inputStream.readFully(getFilePointer(), b, offset, length);
      }
    }

    @Override
    public Object clone() {
      DirectIOHdfsIndexInput clone = (DirectIOHdfsIndexInput) super.clone();
      clone.isClone = true;
      return clone;
    }
  }
}
