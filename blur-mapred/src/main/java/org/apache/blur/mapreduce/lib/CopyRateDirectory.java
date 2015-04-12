package org.apache.blur.mapreduce.lib;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Decorator of Directory to capture the copy rate of a directory copy.
 */
public class CopyRateDirectory extends Directory {

  private final Directory _directory;
  private final RateCounter _copyRateCounter;

  public CopyRateDirectory(Directory dir, RateCounter copyRateCounter) {
    _directory = dir;
    _copyRateCounter = copyRateCounter;
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return wrap(_directory.createOutput(name, context));
  }

  private IndexOutput wrap(IndexOutput output) {
    return new CopyRateIndexOutput(output, _copyRateCounter);
  }

  static class CopyRateIndexOutput extends IndexOutput {

    private final IndexOutput _indexOutput;
    private final RateCounter _copyRateCounter;

    public CopyRateIndexOutput(IndexOutput output, RateCounter copyRateCounter) {
      _indexOutput = output;
      _copyRateCounter = copyRateCounter;
    }

    public void copyBytes(DataInput input, long numBytes) throws IOException {
      _indexOutput.copyBytes(input, numBytes);
      if (_copyRateCounter != null) {
        _copyRateCounter.mark(numBytes);
      }
    }

    public void writeByte(byte b) throws IOException {
      _indexOutput.writeByte(b);
      if (_copyRateCounter != null) {
        _copyRateCounter.mark();
      }
    }

    public void flush() throws IOException {
      _indexOutput.flush();
    }

    public void close() throws IOException {
      _indexOutput.close();
    }

    public long getFilePointer() {
      return _indexOutput.getFilePointer();
    }

    @SuppressWarnings("deprecation")
    public void seek(long pos) throws IOException {
      _indexOutput.seek(pos);
    }

    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      _indexOutput.writeBytes(b, offset, length);
      _copyRateCounter.mark(length);
    }

    public long length() throws IOException {
      return _indexOutput.length();
    }
  }

  public String[] listAll() throws IOException {
    return _directory.listAll();
  }

  public boolean fileExists(String name) throws IOException {
    return _directory.fileExists(name);
  }

  public void deleteFile(String name) throws IOException {
    _directory.deleteFile(name);
  }

  public long fileLength(String name) throws IOException {
    return _directory.fileLength(name);
  }

  public void sync(Collection<String> names) throws IOException {
    _directory.sync(names);
  }

  public IndexInput openInput(String name, IOContext context) throws IOException {
    return _directory.openInput(name, context);
  }

  public void close() throws IOException {
    _directory.close();
  }

}
