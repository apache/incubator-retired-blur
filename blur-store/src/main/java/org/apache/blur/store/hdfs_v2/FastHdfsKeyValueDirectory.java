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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.memory.MemoryLeakDetector;
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

  private static final String MISSING_METADATA_MESSAGE = "Missing meta data for file [{0}], setting length to '0'.  This can occur when a kv log files writes across blocks in hdfs.";

  private static final long GC_DELAY = TimeUnit.HOURS.toMillis(1);

  private static final Log LOG = LogFactory.getLog(FastHdfsKeyValueDirectory.class);

  private static final String LASTMOD = "/lastmod";
  private static final String LENGTH = "/length";
  private static final BytesRef FILES = new BytesRef("FILES");
  private static final String SEP = "|";

  private final Map<String, Long> _files = new ConcurrentHashMap<String, Long>();
  private final HdfsKeyValueStore _store;
  private final int _blockSize = 4096;
  private final Path _path;
  private long _lastGc;

  public FastHdfsKeyValueDirectory(Timer hdfsKeyValueTimer, Configuration configuration, Path path) throws IOException {
    _path = path;
    _store = new HdfsKeyValueStore(hdfsKeyValueTimer, configuration, path);
    MemoryLeakDetector.record(_store, "HdfsKeyValueStore", path.toString());
    BytesRef value = new BytesRef();
    if (_store.get(FILES, value)) {
      String filesString = value.utf8ToString();
      // System.out.println("Open Files String [" + filesString + "]");
      String[] files = filesString.split("\\" + SEP);
      for (String file : files) {
        if (file.isEmpty()) {
          throw new IOException("Empty file names should not occur [" + filesString + "]");
        }
        BytesRef key = new BytesRef(file + LENGTH);
        if (_store.get(key, value)) {
          _files.put(file, Long.parseLong(value.utf8ToString()));
        } else {
          LOG.warn(MISSING_METADATA_MESSAGE, file);
        }
      }
    }
    setLockFactory(NoLockFactory.getNoLockFactory());
    writeFilesNames();
    gc();
  }

  public void gc() throws IOException {
    LOG.info("Running GC over the hdfs kv directory [{0}].", _path);
    Iterable<Entry<BytesRef, BytesRef>> scan = _store.scan(null);
    List<BytesRef> toBeDeleted = new ArrayList<BytesRef>();
    for (Entry<BytesRef, BytesRef> e : scan) {
      BytesRef bytesRef = e.getKey();
      if (bytesRef.equals(FILES)) {
        continue;
      }
      String key = bytesRef.utf8ToString();
      int indexOf = key.indexOf('/');
      if (indexOf < 0) {
        LOG.error("Unknown key type in hdfs kv store [" + key + "]");
      } else {
        String filename = key.substring(0, indexOf);
        if (!_files.containsKey(filename)) {
          toBeDeleted.add(bytesRef);
        }
      }
    }
    for (BytesRef key : toBeDeleted) {
      _store.delete(key);
    }
    _lastGc = System.currentTimeMillis();
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
    Set<String> fileNames = new TreeSet<String>(_files.keySet());
    for (String n : fileNames) {
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
    if (fileExists(name)) {
      deleteFile(name);
    }
    return new FastHdfsKeyValueIndexOutput(name, _blockSize, this);
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> fileNames = new HashSet<String>(_files.keySet());
    return fileNames.toArray(new String[fileNames.size()]);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    boolean containsKey = _files.containsKey(name);
    LOG.debug("FileExists [{0}] [{1}].", name, containsKey);
    return containsKey;
  }

  @Override
  public void deleteFile(String name) throws IOException {
    Long length = _files.remove(name);
    if (length != null) {
      LOG.debug("Removing file [{0}] with length [{1}].", name, length);
      long blocks = length / _blockSize;
      _store.delete(new BytesRef(name + LENGTH));
      _store.delete(new BytesRef(name + LASTMOD));
      for (long l = 0; l <= blocks; l++) {
        _store.delete(new BytesRef(name + "/" + l));
      }
      writeFileNamesAndSync();
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
    writeFileNamesAndSync();
    if (shouldPerformGC()) {
      gc();
    }
  }

  private void writeFileNamesAndSync() throws IOException {
    writeFilesNames();
    _store.sync();
  }

  private boolean shouldPerformGC() {
    if (_lastGc + GC_DELAY < System.currentTimeMillis()) {
      return true;
    }
    return false;
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
