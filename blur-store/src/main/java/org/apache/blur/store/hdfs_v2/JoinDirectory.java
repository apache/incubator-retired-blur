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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.store.hdfs.HdfsSymlink;
import org.apache.blur.utils.BlurConstants;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class JoinDirectory extends Directory implements LastModified, HdfsSymlink {

  private final HdfsDirectory _longTermStorage;
  private final Directory _shortTermStorage;
  private final Set<String> _shortTermSyncFiles = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final Set<String> _longTermSyncFiles = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  public JoinDirectory(HdfsDirectory longTermStorage, Directory shortTermStorage) throws IOException {
    lastModifiedCheck(longTermStorage);
    lastModifiedCheck(shortTermStorage);
    _longTermStorage = longTermStorage;
    _shortTermStorage = shortTermStorage;
    setLockFactory(_longTermStorage.getLockFactory());
  }

  private void lastModifiedCheck(Directory directory) {
    if (!(directory instanceof LastModified)) {
      throw new RuntimeException("Directory [" + directory + "] does not implement '" + LastModified.class.toString()
          + "'");
    }
  }

  @Override
  public String[] listAll() throws IOException {
    Set<String> names = new HashSet<String>();
    for (String s : _longTermStorage.listAll()) {
      names.add(s);
    }
    for (String s : _shortTermStorage.listAll()) {
      names.add(s);
    }
    return names.toArray(new String[names.size()]);
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    if (_shortTermStorage.fileExists(name)) {
      return true;
    }
    return _longTermStorage.fileExists(name);
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (_shortTermStorage.fileExists(name)) {
      _shortTermStorage.deleteFile(name);
      return;
    }
    _longTermStorage.deleteFile(name);
  }

  @Override
  public long fileLength(String name) throws IOException {
    if (_shortTermStorage.fileExists(name)) {
      return _shortTermStorage.fileLength(name);
    }
    return _longTermStorage.fileLength(name);
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (shouldBeLongTermStorage()) {
      addLongTermSyncFile(name);
      return _longTermStorage.createOutput(name, context);
    }
    addShortTermSyncFile(name);
    return _shortTermStorage.createOutput(name, context);
  }

  private boolean shouldBeLongTermStorage() {
    if (Thread.currentThread().getName().startsWith(BlurConstants.SHARED_MERGE_SCHEDULER_PREFIX)) {
      return true;
    }
    return StoreDirection.LONG_TERM.get();
  }

  private void addShortTermSyncFile(String name) {
    _shortTermSyncFiles.add(name);
  }

  private void addLongTermSyncFile(String name) {
    _longTermSyncFiles.add(name);
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    Set<String> shortNames = new HashSet<String>();
    Set<String> longNames = new HashSet<String>();

    for (String name : names) {
      if (_shortTermSyncFiles.contains(name)) {
        _shortTermSyncFiles.remove(name);
        shortNames.add(name);
      }
      if (_longTermSyncFiles.contains(name)) {
        _longTermSyncFiles.remove(name);
        longNames.add(name);
      }
    }

    if (!shortNames.isEmpty()) {
      // System.out.println("Sync short [" + shortNames + "]");
      _shortTermStorage.sync(shortNames);
    }

    if (!longNames.isEmpty()) {
      // System.out.println("Sync long [" + longNames + "]");
      _longTermStorage.sync(longNames);
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (_shortTermStorage.fileExists(name)) {
      return _shortTermStorage.openInput(name, context);
    }
    return _longTermStorage.openInput(name, context);
  }

  @Override
  public void close() throws IOException {
    _shortTermStorage.close();
    _longTermStorage.close();
  }

  @Override
  public long getFileModified(String name) throws IOException {
    if (_shortTermStorage.fileExists(name)) {
      return ((LastModified) _shortTermStorage).getFileModified(name);
    }
    return ((LastModified) _longTermStorage).getFileModified(name);
  }

  @Override
  public HdfsDirectory getSymlinkDirectory() {
    return _longTermStorage;
  }

}
