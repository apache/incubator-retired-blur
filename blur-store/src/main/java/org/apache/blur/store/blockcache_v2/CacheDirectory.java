/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.store.blockcache_v2;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class CacheDirectory extends Directory implements DirectoryDecorator, LastModified {

  private final Directory _internal;
  private final String _directoryName;
  private final Cache _cache;
  private final String _shard;
  private final String _table;
  private final Set<String> _tableBlockCacheFileTypes;

  public CacheDirectory(String table, String shard, Directory directory, Cache cache,
      Set<String> tableBlockCacheFileTypes) {
    if (!(directory instanceof LastModified)) {
      throw new RuntimeException("Directory [" + directory + "] does not implement '" + LastModified.class.toString()
          + "'");
    }
    _table = table;
    _shard = shard;
    _directoryName = notNull(table + "_" + shard);
    _internal = notNull(directory);
    _cache = notNull(cache);
    _tableBlockCacheFileTypes = tableBlockCacheFileTypes;
  }

  public String getShard() {
    return _shard;
  }

  public String getTable() {
    return _table;
  }

  public IndexInput openInput(String name, IOContext context) throws IOException {
    IndexInput indexInput = _internal.openInput(name, context);
    if (_cache.cacheFileForReading(this, name, context) || isCachableFile(name)) {
      return new CacheIndexInput(this, name, indexInput, _cache);
    }
    return indexInput;
  }

  private boolean isCachableFile(String name) {
    if (_tableBlockCacheFileTypes == null) {
      return true;
    } else if (_tableBlockCacheFileTypes.isEmpty()) {
      return false;
    }
    for (String ext : _tableBlockCacheFileTypes) {
      if (name.endsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (_cache.cacheFileForWriting(this, name, context) || isCachableFile(name)) {
      return new CacheIndexOutput(this, name, _cache, _internal, context);
    }
    return _internal.createOutput(name, context);
  }

  public void deleteFile(String name) throws IOException {
    _cache.removeFile(this, name);
    _internal.deleteFile(name);
  }

  public String[] listAll() throws IOException {
    return _internal.listAll();
  }

  public boolean fileExists(String name) throws IOException {
    return _internal.fileExists(name);
  }

  public long fileLength(String name) throws IOException {
    return _internal.fileLength(name);
  }

  public void sync(Collection<String> names) throws IOException {
    _internal.sync(names);
  }

  public Lock makeLock(String name) {
    return _internal.makeLock(name);
  }

  public void clearLock(String name) throws IOException {
    _internal.clearLock(name);
  }

  public void close() throws IOException {
    _cache.releaseDirectory(this);
    _internal.close();
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _internal.setLockFactory(lockFactory);
  }

  public LockFactory getLockFactory() {
    return _internal.getLockFactory();
  }

  public String getLockID() {
    return _internal.getLockID();
  }

  public String toString() {
    return _internal.toString();
  }

  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    _internal.copy(to, src, dest, context);
  }

  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    return _internal.createSlicer(name, context);
  }

  public String getDirectoryName() {
    return _directoryName;
  }

  @Override
  public long getFileModified(String name) throws IOException {
    return ((LastModified) _internal).getFileModified(name);
  }

  @Override
  public Directory getOriginalDirectory() {
    return _internal;
  }

  private static <T> T notNull(T t) {
    if (t == null) {
      throw new IllegalArgumentException("Cannot be null");
    }
    return t;
  }

}