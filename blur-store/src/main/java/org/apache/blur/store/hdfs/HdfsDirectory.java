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
import java.io.UnsupportedEncodingException;
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

public class HdfsDirectory extends Directory implements LastModified, HdfsSymlink {

  public static final String LNK = ".lnk";

  private static final String UTF_8 = "UTF-8";

  private static final Log LOG = LogFactory.getLog(HdfsDirectory.class);

  /**
   * We keep the metrics separate per filesystem.
   */
  protected static Map<URI, MetricsGroup> _metricsGroupMap = new WeakHashMap<URI, MetricsGroup>();

  protected final Path _path;
  protected final FileSystem _fileSystem;
  protected final MetricsGroup _metricsGroup;
  protected final Map<String, Long> _fileLengthMap = new ConcurrentHashMap<String, Long>();
  protected final Map<String, Boolean> _symlinkMap = new ConcurrentHashMap<String, Boolean>();
  protected final Map<String, Path> _symlinkPathMap = new ConcurrentHashMap<String, Path>();
  protected final Map<Path, FSDataInputStream> _inputMap = new ConcurrentHashMap<Path, FSDataInputStream>();
  protected final boolean _useCache = true;

  public HdfsDirectory(Configuration configuration, Path path) throws IOException {
    _fileSystem = path.getFileSystem(configuration);
    _path = _fileSystem.makeQualified(path);
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
          Path p = fileStatus.getPath();
          String name = p.getName();
          if (name.endsWith(LNK)) {
            String resolvedName = getRealFileName(name);
            Path resolvedPath = getPath(resolvedName);
            FileStatus resolvedFileStatus = _fileSystem.getFileStatus(resolvedPath);
            _fileLengthMap.put(resolvedName, resolvedFileStatus.getLen());
          } else {
            _fileLengthMap.put(name, fileStatus.getLen());
          }
        }
      }
    }
  }

  private String getRealFileName(String name) {
    if (name.endsWith(LNK)) {
      int lastIndexOf = name.lastIndexOf(LNK);
      return name.substring(0, lastIndexOf);
    }
    return name;
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
    _fileLengthMap.put(name, 0L);
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
        _fileLengthMap.put(name, outputStream.getPos());
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
      Set<String> names = new HashSet<String>(_fileLengthMap.keySet());
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
      return _fileLengthMap.containsKey(name);
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
        _fileLengthMap.remove(name);
      }
      delete(name);
    } else {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
  }

  protected void delete(String name) throws IOException {
    Path path = getPathOrSymlink(name);
    FSDataInputStream inputStream = _inputMap.remove(path);
    Tracer trace = Trace.trace("filesystem - delete", Trace.param("path", path));
    if (inputStream != null) {
      inputStream.close();
    }
    if (_useCache) {
      _symlinkMap.remove(name);
      _symlinkPathMap.remove(name);
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
      Long length = _fileLengthMap.get(name);
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

  private Path getPath(String name) throws IOException {
    if (isSymlink(name)) {
      return getRealFilePathFromSymlink(name);
    }
    return new Path(_path, name);
  }

  private Path getPathOrSymlink(String name) throws IOException {
    if (isSymlink(name)) {
      return new Path(_path, name + LNK);
    }
    return new Path(_path, name);
  }

  private Path getRealFilePathFromSymlink(String name) throws IOException {
    // need to cache
    if (_useCache) {
      Path path = _symlinkPathMap.get(name);
      if (path != null) {
        return path;
      }
    }
    Tracer trace = Trace.trace("filesystem - getRealFilePathFromSymlink", Trace.param("name", name));
    try {
      Path linkPath = new Path(_path, name + LNK);
      Path path = readRealPathDataFromSymlinkPath(_fileSystem, linkPath);
      if (_useCache) {
        _symlinkPathMap.put(name, path);
      }
      return path;
    } finally {
      trace.done();
    }
  }

  public static Path readRealPathDataFromSymlinkPath(FileSystem fileSystem, Path linkPath) throws IOException,
      UnsupportedEncodingException {
    FileStatus fileStatus = fileSystem.getFileStatus(linkPath);
    FSDataInputStream inputStream = fileSystem.open(linkPath);
    byte[] buf = new byte[(int) fileStatus.getLen()];
    inputStream.readFully(buf);
    inputStream.close();
    Path path = new Path(new String(buf, UTF_8));
    return path;
  }

  private boolean isSymlink(String name) throws IOException {
    if (_useCache) {
      Boolean b = _symlinkMap.get(name);
      if (b != null) {
        return b;
      }
    }
    Tracer trace = Trace.trace("filesystem - isSymlink", Trace.param("name", name));
    try {
      boolean exists = _fileSystem.exists(new Path(_path, name + LNK));
      if (_useCache) {
        _symlinkMap.put(name, exists);
      }
      return exists;
    } finally {
      trace.done();
    }
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
      // Unwrap original directory
      copy(((DirectoryDecorator) to).getOriginalDirectory(), src, dest, context);
      return;
    } else if (to instanceof HdfsSymlink) {
      // Attempt to create a symlink and return.
      if (createSymLink(((HdfsSymlink) to).getSymlinkDirectory(), src, dest)) {
        return;
      }
    }
    // if all else fails, just copy the file.
    super.copy(to, src, dest, context);
  }

  private boolean createSymLink(HdfsDirectory to, String src, String dest) throws IOException {
    Path srcPath = getPath(src);
    Path destDir = to.getPath();
    LOG.info("Creating symlink with name [{0}] to [{1}]", dest, srcPath);
    FSDataOutputStream outputStream = _fileSystem.create(getSymPath(destDir, dest));
    outputStream.write(srcPath.toString().getBytes(UTF_8));
    outputStream.close();
    if (_useCache) {
      to._fileLengthMap.put(dest, _fileLengthMap.get(src));
    }
    return true;
  }

  private Path getSymPath(Path destDir, String destFilename) {
    return new Path(destDir, destFilename + LNK);
  }

  @Override
  public HdfsDirectory getSymlinkDirectory() {
    return this;
  }

}
