/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.hdfs_v2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.BytesRef;

public class FastHdfsKeyValueDirectory extends Directory implements LastModified {

  private static final String LASTMOD = "/lastmod";
  private static final String LENGTH = "/length";
  private static final BytesRef FILES = new BytesRef("FILES");
  private static final String SEP = "|";
  private final Map<String, Long> _files = new ConcurrentHashMap<String, Long>();
  private final HdfsKeyValueStore _store;
  private final int _blockSize = 4096;

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration();
    Path path = new Path("hdfs://localhost:9000/blur/fast/shard-00000000/fast");
    FastHdfsKeyValueDirectory dir = new FastHdfsKeyValueDirectory(configuration, path);
    HdfsKeyValueStore store = dir._store;
    store.cleanupOldFiles();
    String[] listAll = dir.listAll();
    long total = 0;
    for (String s : listAll) {
      long fileLength = dir.fileLength(s);
      System.out.println(s + " " + fileLength);
      total += fileLength;
    }
    System.out.println("Total [" + total + "]");
    dir.close();

  }

  public FastHdfsKeyValueDirectory(Configuration configuration, Path path) throws IOException {
    _store = new HdfsKeyValueStore(configuration, path);
    BytesRef value = new BytesRef();
    if (_store.get(FILES, value)) {
      String filesString = value.utf8ToString();
      String[] files = filesString.split("\\" + SEP);
      for (String file : files) {
        if (file.isEmpty()) {
          continue;
        }
        BytesRef key = new BytesRef(file + LENGTH);
        if (_store.get(key, value)) {
          _files.put(file, Long.parseLong(value.utf8ToString()));
        } else {
          throw new IOException("Missing meta data for [" + key.utf8ToString() + "].");
        }
      }
    }
    setLockFactory(NoLockFactory.getNoLockFactory());
  }

  public void writeBlock(String name, long blockId, byte[] b, int offset, int length) throws IOException {
    _store.put(new BytesRef(name + "/" + blockId), new BytesRef(b, offset, length));
  }

  public void readBlock(String name, long blockId, BytesRef ref) throws IOException {
    if (!_store.get(new BytesRef(name + "/" + blockId), ref)) {
      throw new IOException("Block [" + name + "] [" + blockId + "] not found.");
    }
  }

  public synchronized void writeLength(String name, long length) throws IOException {
    _files.put(name, length);
    _store.put(new BytesRef(name + LENGTH), new BytesRef(Long.toString(length)));
    _store.put(new BytesRef(name + LASTMOD), new BytesRef(Long.toString(System.currentTimeMillis())));
    writeFilesNames();
  }

  private void writeFilesNames() throws IOException {
    StringBuilder builder = new StringBuilder();
    for (String n : _files.keySet()) {
      if (builder.length() != 0) {
        builder.append(SEP);
      }
      builder.append(n);
    }
    _store.put(FILES, new BytesRef(builder.toString()));
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new FastHdfsKeyValueIndexInput(name, fileLength(name), _blockSize, this);
  }

  @Override
  public IndexOutput createOutput(final String name, IOContext context) throws IOException {
    return new FastHdfsKeyValueIndexOutput(name, _blockSize, this);
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> fileNames = new HashSet<String>(_files.keySet());
    fileNames.remove(null);
    return fileNames.toArray(new String[fileNames.size()]);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    return _files.containsKey(name);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Long length = _files.remove(name);
    if (length != null) {
      long blocks = length / _blockSize;
      _store.delete(new BytesRef(name + LENGTH));
      _store.delete(new BytesRef(name + LASTMOD));
      for (long l = 0; l <= blocks; l++) {
        _store.delete(new BytesRef(name + "/" + l));
      }
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (fileExists(name)) {
      return _files.get(name);
    }
    throw new FileNotFoundException(name);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    _store.sync();
  }

  @Override
  public void close() throws IOException {
    _store.close();
  }

  @Override
  public long getFileModified(String name) throws IOException {
    BytesRef value = new BytesRef();
    if (_store.get(new BytesRef(name + LASTMOD), value)) {
      return Long.parseLong(value.utf8ToString());
    }
    throw new FileNotFoundException(name);
  }

}
