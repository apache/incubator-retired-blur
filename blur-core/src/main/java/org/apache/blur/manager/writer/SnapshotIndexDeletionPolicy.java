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
package org.apache.blur.manager.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

public class SnapshotIndexDeletionPolicy extends IndexDeletionPolicy {

  private static final Log LOG = LogFactory.getLog(SnapshotIndexDeletionPolicy.class);

  private final Configuration _configuration;
  private final Path _path;
  private final Map<String, Long> _namesToGenerations = new ConcurrentHashMap<String, Long>();
  private final Map<Long, Set<String>> _generationsToNames = new ConcurrentHashMap<Long, Set<String>>();
  private final WriteLock _writeLock = new ReentrantReadWriteLock().writeLock();

  public SnapshotIndexDeletionPolicy(Configuration configuration, Path path) throws IOException {
    _configuration = configuration;
    _path = path;
    FileSystem fileSystem = _path.getFileSystem(configuration);
    fileSystem.mkdirs(path);
    loadGenerations();
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    onCommit(commits);
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    _writeLock.lock();
    try {
      int size = commits.size();
      for (int i = 0; i < size - 1; i++) {
        IndexCommit indexCommit = commits.get(i);
        long generation = indexCommit.getGeneration();
        if (!_generationsToNames.containsKey(generation)) {
          indexCommit.delete();
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private synchronized void storeGenerations() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(_path);
    SortedSet<FileStatus> existing = new TreeSet<FileStatus>(Arrays.asList(listStatus));
    long currentFile;
    if (!existing.isEmpty()) {
      FileStatus last = existing.last();
      currentFile = Long.parseLong(last.getPath().getName());
    } else {
      currentFile = 0;
    }
    Path path = new Path(_path, buffer(currentFile + 1));
    LOG.info("Creating new snapshot file [{0}]", path);
    FSDataOutputStream outputStream = fileSystem.create(path, false);
    Writer writer = SequenceFile.createWriter(_configuration, outputStream, Text.class, LongWritable.class,
        CompressionType.NONE, null);
    for (Entry<String, Long> e : _namesToGenerations.entrySet()) {
      writer.append(new Text(e.getKey()), new LongWritable(e.getValue()));
    }
    writer.close();
    outputStream.close();
    cleanupOldFiles(fileSystem, existing);
  }

  private void cleanupOldFiles(FileSystem fileSystem, SortedSet<FileStatus> existing) throws IOException {
    for (FileStatus fileStatus : existing) {
      fileSystem.delete(fileStatus.getPath(), false);
    }
  }

  private String buffer(long number) {
    String s = Long.toString(number);
    StringBuilder builder = new StringBuilder();
    for (int i = s.length(); i < 12; i++) {
      builder.append('0');
    }
    return builder.append(s).toString();
  }

  private void loadGenerations() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    FileStatus[] listStatus = fileSystem.listStatus(_path);
    SortedSet<FileStatus> existing = new TreeSet<FileStatus>(Arrays.asList(listStatus));
    if (existing.isEmpty()) {
      return;
    }
    FileStatus last = existing.last();
    Reader reader = new SequenceFile.Reader(fileSystem, last.getPath(), _configuration);
    Text key = new Text();
    LongWritable value = new LongWritable();
    while (reader.next(key, value)) {
      String name = key.toString();
      long gen = value.get();
      _namesToGenerations.put(name, gen);
      Set<String> names = _generationsToNames.get(gen);
      if (names == null) {
        names = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        _generationsToNames.put(gen, names);
      }
      names.add(name);
    }
    reader.close();
    existing.remove(last);
    cleanupOldFiles(fileSystem, existing);
  }

  public void createSnapshot(String name, DirectoryReader reader, String context) throws IOException {
    _writeLock.lock();
    try {
      if (_namesToGenerations.containsKey(name)) {
        throw new IOException("Snapshot [" + name + "] already exists.");
      }
      LOG.info("Creating snapshot [{0}] in [{1}].", name, context);
      IndexCommit indexCommit = reader.getIndexCommit();
      long generation = indexCommit.getGeneration();
      _namesToGenerations.put(name, generation);
      Set<String> names = _generationsToNames.get(generation);
      if (names == null) {
        names = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        _generationsToNames.put(generation, names);
      }
      names.add(name);
      storeGenerations();
    } finally {
      _writeLock.unlock();
    }
  }

  public void removeSnapshot(String name, String context) throws IOException {
    _writeLock.lock();
    try {
      Long gen = _namesToGenerations.get(name);
      if (gen == null) {
        LOG.info("Snapshot [{0}] does not exist in [{1}].", name, context);
        return;
      }
      LOG.info("Removing snapshot [{0}] from [{1}].", name, context);
      _namesToGenerations.remove(name);
      Set<String> names = _generationsToNames.get(gen);
      names.remove(name);
      if (names.isEmpty()) {
        _generationsToNames.remove(gen);
      }
      storeGenerations();
    } finally {
      _writeLock.unlock();
    }
  }

  public Collection<String> getSnapshots() {
    return new HashSet<String>(_namesToGenerations.keySet());
  }

  public Path getSnapshotsDirectoryPath() {
    return _path;
  }

  public Long getGeneration(String name) {
    return _namesToGenerations.get(name);
  }

  public static Path getGenerationsPath(Path shardDir) {
    return new Path(shardDir, "generations");
  }
}
