package org.apache.blur.store.hdfs;

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

import static org.apache.blur.metrics.MetricsConstants.HDFS;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.blockcache.LastModified;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

public class HdfsDirectory extends Directory implements LastModified, HdfsQuickMove {

  private static final Log LOG = LogFactory.getLog(HdfsDirectory.class);

  /**
   * We keep the metrics separate per filesystem.
   */
  protected static Map<URI, MetricsGroup> _metricsGroupMap = new WeakHashMap<URI, MetricsGroup>();

  protected final Path _path;
  protected final FileSystem _fileSystem;
  protected final MetricsGroup _metricsGroup;
  protected final Map<String, Long> _fileMap = new ConcurrentHashMap<String, Long>();
  protected final Map<Path, FSDataInputStream> _inputMap = new ConcurrentHashMap<Path, FSDataInputStream>();
  protected final boolean _useCache = true;

  public HdfsDirectory(Configuration configuration, Path path) throws IOException {
    this._path = path;
    _fileSystem = path.getFileSystem(configuration);
    _fileSystem.mkdirs(path);
    setLockFactory(NoLockFactory.getNoLockFactory());
    synchronized (_metricsGroupMap) {
      URI uri = _fileSystem.getUri();
      MetricsGroup metricsGroup = _metricsGroupMap.get(uri);
      if (metricsGroup == null) {
        String scope = uri.toString();
        metricsGroup = createNewMetricsGroup(scope);
        _metricsGroupMap.put(uri, metricsGroup);
      }
      _metricsGroup = metricsGroup;
    }
    if (_useCache) {
      FileStatus[] listStatus = _fileSystem.listStatus(_path);
      for (FileStatus fileStatus : listStatus) {
        if (!fileStatus.isDir()) {
          _fileMap.put(fileStatus.getPath().getName(), fileStatus.getLen());
        }
      }
    }
  }

  private MetricsGroup createNewMetricsGroup(String scope) {
    MetricName readAccessName = new MetricName(ORG_APACHE_BLUR, HDFS, "Read Latency in \u00B5s", scope);
    MetricName writeAcccessName = new MetricName(ORG_APACHE_BLUR, HDFS, "Write Latency in \u00B5s", scope);
    MetricName readThroughputName = new MetricName(ORG_APACHE_BLUR, HDFS, "Read Throughput", scope);
    MetricName writeThroughputName = new MetricName(ORG_APACHE_BLUR, HDFS, "Write Throughput", scope);

    Histogram readAccess = Metrics.newHistogram(readAccessName);
    Histogram writeAccess = Metrics.newHistogram(writeAcccessName);
    Meter readThroughput = Metrics.newMeter(readThroughputName, "Read Bytes", TimeUnit.SECONDS);
    Meter writeThroughput = Metrics.newMeter(writeThroughputName, "Write Bytes", TimeUnit.SECONDS);
    return new MetricsGroup(readAccess, writeAccess, readThroughput, writeThroughput);
  }

  @Override
  public String toString() {
    return "HdfsDirectory path=[" + _path + "]";
  }

  @Override
  public IndexOutput createOutput(final String name, IOContext context) throws IOException {
    LOG.debug("createOutput [{0}] [{1}] [{2}]", name, context, _path);
    if (fileExists(name)) {
      throw new IOException("File [" + name + "] already exists found.");
    }
    _fileMap.put(name, 0L);
    final FSDataOutputStream outputStream = openForOutput(name);
    return new BufferedIndexOutput() {

      @Override
      public long length() throws IOException {
        return outputStream.getPos();
      }

      @Override
      protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
        long start = System.nanoTime();
        outputStream.write(b, offset, len);
        long end = System.nanoTime();
        _metricsGroup.writeAccess.update((end - start) / 1000);
        _metricsGroup.writeThroughput.mark(len);
      }

      @Override
      public void close() throws IOException {
        super.close();
        _fileMap.put(name, outputStream.getPos());
        outputStream.close();
        openForInput(name);
      }

      @Override
      public void seek(long pos) throws IOException {
        throw new IOException("seeks not allowed on IndexOutputs.");
      }
    };
  }

  protected FSDataOutputStream openForOutput(String name) throws IOException {
    Path path = getPath(name);
    Tracer trace = Trace.trace("filesystem - create", Trace.param("path", path));
    try {
      return _fileSystem.create(path);
    } finally {
      trace.done();
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    LOG.debug("openInput [{0}] [{1}] [{2}]", name, context, _path);
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found.");
    }
    FSDataInputStream inputStream = openForInput(name);
    long fileLength = fileLength(name);
    return new HdfsIndexInput(name, inputStream, fileLength, _metricsGroup, getPath(name));
  }

  protected synchronized FSDataInputStream openForInput(String name) throws IOException {
    Path path = getPath(name);
    FSDataInputStream inputStream = _inputMap.get(path);
    if (inputStream != null) {
      return inputStream;
    }
    Tracer trace = Trace.trace("filesystem - open", Trace.param("path", path));
    try {
      inputStream = _fileSystem.open(path);
      _inputMap.put(path, inputStream);
      return inputStream;
    } finally {
      trace.done();
    }
  }

  @Override
  public String[] listAll() throws IOException {
    LOG.debug("listAll [{0}]", _path);

    if (_useCache) {
      Set<String> names = new HashSet<String>(_fileMap.keySet());
      return names.toArray(new String[names.size()]);
    }

    Tracer trace = Trace.trace("filesystem - list", Trace.param("path", _path));
    try {
      FileStatus[] files = _fileSystem.listStatus(_path, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          try {
            return _fileSystem.isFile(path);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      String[] result = new String[files.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = files[i].getPath().getName();
      }
      return result;
    } finally {
      trace.done();
    }
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    LOG.debug("fileExists [{0}] [{1}]", name, _path);
    if (_useCache) {
      return _fileMap.containsKey(name);
    }
    return exists(name);
  }

  protected boolean exists(String name) throws IOException {
    Path path = getPath(name);
    Tracer trace = Trace.trace("filesystem - exists", Trace.param("path", path));
    try {
      return _fileSystem.exists(path);
    } finally {
      trace.done();
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    LOG.debug("deleteFile [{0}] [{1}]", name, _path);
    if (fileExists(name)) {
      if (_useCache) {
        _fileMap.remove(name);
      }
      delete(name);
    } else {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
  }

  protected void delete(String name) throws IOException {
    Path path = getPath(name);
    FSDataInputStream inputStream = _inputMap.remove(path);
    Tracer trace = Trace.trace("filesystem - delete", Trace.param("path", path));
    if (inputStream != null) {
      inputStream.close();
    }
    try {
      _fileSystem.delete(path, true);
    } finally {
      trace.done();
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    LOG.debug("fileLength [{0}] [{1}]", name, _path);
    if (_useCache) {
      Long length = _fileMap.get(name);
      if (length == null) {
        throw new FileNotFoundException(name);
      }
      return length;
    }

    return length(name);
  }

  protected long length(String name) throws IOException {
    Path path = getPath(name);
    Tracer trace = Trace.trace("filesystem - length", Trace.param("path", path));
    try {
      return _fileSystem.getFileStatus(path).getLen();
    } finally {
      trace.done();
    }
  }

  @Override
  public void sync(Collection<String> names) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  public Path getPath() {
    return _path;
  }

  private Path getPath(String name) {
    return new Path(_path, name);
  }

  public long getFileModified(String name) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
    return fileModified(name);
  }

  protected long fileModified(String name) throws IOException {
    Path path = getPath(name);
    Tracer trace = Trace.trace("filesystem - fileModified", Trace.param("path", path));
    try {
      return _fileSystem.getFileStatus(path).getModificationTime();
    } finally {
      trace.done();
    }
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    if (to instanceof DirectoryDecorator) {
      copy(((DirectoryDecorator) to).getOriginalDirectory(), src, dest, context);
    } else if (to instanceof HdfsQuickMove) {
      if (quickMove(((HdfsQuickMove) to).getQuickMoveDirectory(), src, dest, context)) {
        return;
      }
    } else {
      slowCopy(to, src, dest, context);
    }
  }

  protected void slowCopy(Directory to, String src, String dest, IOContext context) throws IOException {
    super.copy(to, src, dest, context);
  }

  private boolean quickMove(Directory to, String src, String dest, IOContext context) throws IOException {
    LOG.warn("DANGEROUS copy [{0}] [{1}] [{2}] [{3}] [{4}]", to, src, dest, context, _path);
    HdfsDirectory simpleTo = (HdfsDirectory) to;
    if (ifSameCluster(simpleTo, this)) {
      Path newDest = simpleTo.getPath(dest);
      Path oldSrc = getPath(src);
      if (_useCache) {
        simpleTo._fileMap.put(dest, _fileMap.get(src));
        _fileMap.remove(src);
        FSDataInputStream inputStream = _inputMap.get(src);
        if (inputStream != null) {
          inputStream.close();
        }
      }
      return _fileSystem.rename(oldSrc, newDest);
    }
    return false;
  }

  private boolean ifSameCluster(HdfsDirectory dest, HdfsDirectory src) {
    // @TODO
    return true;
  }

  @Override
  public HdfsDirectory getQuickMoveDirectory() {
    return this;
  }

}
