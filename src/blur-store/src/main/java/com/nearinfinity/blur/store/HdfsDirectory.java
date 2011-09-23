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

package com.nearinfinity.blur.store;

import static com.nearinfinity.blur.store.Constants.BUFFER_SIZE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class HdfsDirectory extends Directory {

  protected Path _hdfsDirPath;
  protected AtomicReference<FileSystem> _fileSystemRef = new AtomicReference<FileSystem>();
  protected Configuration _configuration;

  public HdfsDirectory(Path hdfsDirPath) throws IOException {
    _hdfsDirPath = hdfsDirPath;

    _configuration = new Configuration();
    String disableCacheName = String.format("fs.%s.impl.disable.cache", _hdfsDirPath.toUri().getScheme());
    _configuration.setBoolean(disableCacheName, true);

    reopenFileSystem();
    try {
      if (!getFileSystem().exists(hdfsDirPath)) {
        getFileSystem().mkdirs(hdfsDirPath);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void reopenFileSystem() throws IOException {
    FileSystem fileSystem = FileSystem.get(_hdfsDirPath.toUri(), _configuration);
    FileSystem oldFs = _fileSystemRef.get();
    _fileSystemRef.set(fileSystem);
    if (oldFs != null) {
      oldFs.close();
    }
  }

  @Override
  public void close() throws IOException {

  }

  public IndexOutput createOutputHdfs(String name) throws IOException {
    HdfsFileWriter writer = new HdfsFileWriter(getFileSystem(), new Path(_hdfsDirPath,name));
    return new HdfsIndexOutput(writer);
  }

  @Override
  public IndexOutput createOutput(String name) throws IOException {
    return createOutputHdfs(name);
  }

  protected void rename(String currentName, String newName) throws IOException {
    getFileSystem().rename(new Path(_hdfsDirPath, currentName), new Path(_hdfsDirPath, newName));
  }

  protected FSDataOutputStream getOutputStream(String name) throws IOException {
    return getFileSystem().create(new Path(_hdfsDirPath, name));
  }

  @Override
  public IndexInput openInput(String name) throws IOException {
    return openInput(name, BUFFER_SIZE);
  }

  @Override
  public IndexInput openInput(final String name, int bufferSize) throws IOException {
    return openInputHdfs(name, bufferSize);
  }

  public IndexInput openInputHdfs(String name) throws IOException {
    return openInputHdfs(name, BUFFER_SIZE);
  }

  public IndexInput openInputHdfs(String name, int bufferSize) throws IOException {
    //    FSDataInputStream inputStream = getFileSystem().open(new Path(_hdfsDirPath, name));
//    long length = 0;
//    if (inputStream instanceof DFSDataInputStream) {
//      // This is needed because if the file was in progress of being written but
//      // was not closed the
//      // length of the file is 0. This will fetch the synced length of the file.
//      length = ((DFSDataInputStream) inputStream).getVisibleLength();
//    } else {
//      length = fileLength(name);
//    }
    HdfsFileReader reader = new HdfsFileReader(getFileSystem(), new Path(_hdfsDirPath, name));
    return new HdfsIndexInput(reader);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException(name);
    }
    getFileSystem().delete(new Path(_hdfsDirPath, name), false);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return fileExistsHdfs(name);
  }

  public boolean fileExistsHdfs(String name) throws IOException {
    return getFileSystem().exists(new Path(_hdfsDirPath, name));
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException(name);
    }
    return HdfsFileReader.getLength(getFileSystem(), new Path(_hdfsDirPath, name));
  }

  @Override
  public long fileModified(String name) throws IOException {
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
    return files.toArray(new String[] {});
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

  static class HdfsIndexInput extends IndexInput {
    
    private HdfsFileReader _reader;
    private long _length;
    private long _pos;
    private boolean isClone;
    
    public HdfsIndexInput(HdfsFileReader reader) {
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
    public long getFilePointer() {
      return _pos;
    }

    @Override
    public long length() {
      return _length;
    }

    @Override
    public void seek(long pos) throws IOException {
      _pos = pos;
    } 

    @Override
    public byte readByte() throws IOException {
      _reader.seek(_pos);
      try {
        return _reader.readByte();
      } finally {
        _pos++;
      }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
      _reader.seek(_pos);
      try {
        _reader.readBytes(b, offset, len);
      } finally {
        _pos += len;
      }
    }

    @Override
    public Object clone() {
      HdfsIndexInput input = (HdfsIndexInput) super.clone();
      input.isClone = true;
      return input;
    }
  }
  
  static class HdfsIndexOutput extends IndexOutput {
    
    private HdfsFileWriter _writer;
    
    public HdfsIndexOutput(HdfsFileWriter writer) {
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
}
