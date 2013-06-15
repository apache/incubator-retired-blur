package org.apache.blur.lucene.warmup;

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
import java.io.IOException;
import java.util.Collection;

import org.apache.blur.store.hdfs.DirectoryDecorator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

public class TraceableDirectory extends Directory implements DirectoryDecorator {

  private final Directory _dir;
  /**
   * One might feel compelled to make this field volatile because of the
   * massively parallel environment that Blur operates. Please do NOT! This
   * field cannot be volatile for performance reasons. For every query in Lucene
   * there are a number of clones made of each file handle in each segment in
   * each index. If this were volatile every thread would have to check main
   * memory for this field every time a clone was called (see
   * {@link TraceableIndexInput}). In fact I want this field to the remain false
   * in all cases, except for when a warm up trace is needed. If that occurs the
   * Thread running the actual the trace will set this field to true for a brief
   * moment while all the clones are made and the trace is performed (the clones
   * are also made with this this trace thread), this will likely just alter the
   * value for that thread and not commit it back to main memory (heap). After
   * the trace is complete is will return the field to false. During the time
   * when the field is true, if any other field actually reads the value as true
   * there is a second {@link ThreadLocal} field in {@link IndexWarmup} that
   * prevents any other threads from running a trace. However there will be a
   * small performance penalty while that situation occurs, because accessing a
   * {@link ThreadLocal} field is fairly expensive.
   * 
   * In short please leave this the way it is, if you really want to change it
   * please post a question on the mail list first.
   */
  private boolean _trace = false;
  private IndexTracer _indexTracer;

  public TraceableDirectory(Directory dir) {
    _dir = dir;
  }

  public void trace(String name, long filePointer) {
    if (_indexTracer != null) {
      _indexTracer.trace(name, filePointer);
    }
  }

  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new TraceableIndexInput(this, name, context, _dir.openInput(name, context));
  }

  public String[] listAll() throws IOException {
    return _dir.listAll();
  }

  public boolean fileExists(String name) throws IOException {
    return _dir.fileExists(name);
  }

  public void deleteFile(String name) throws IOException {
    _dir.deleteFile(name);
  }

  public long fileLength(String name) throws IOException {
    return _dir.fileLength(name);
  }

  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return _dir.createOutput(name, context);
  }

  public void sync(Collection<String> names) throws IOException {
    _dir.sync(names);
  }

  public void close() throws IOException {
    _dir.close();
  }

  public boolean isTrace() {
    return _trace;
  }

  public void setTrace(boolean trace) {
    _trace = trace;
  }

  public void setIndexTracer(IndexTracer indexTracer) {
    _indexTracer = indexTracer;
  }

  public Lock makeLock(String name) {
    return _dir.makeLock(name);
  }

  public void clearLock(String name) throws IOException {
    _dir.clearLock(name);
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _dir.setLockFactory(lockFactory);
  }

  public LockFactory getLockFactory() {
    return _dir.getLockFactory();
  }

  public String getLockID() {
    return _dir.getLockID();
  }

  public String toString() {
    return _dir.toString();
  }

  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    _dir.copy(to, src, dest, context);
  }

  public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
    return _dir.createSlicer(name, context);
  }

  @Override
  public Directory getOriginalDirectory() {
    return _dir;
  }

}
