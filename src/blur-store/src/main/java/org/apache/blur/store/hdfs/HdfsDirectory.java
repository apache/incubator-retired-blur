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
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.store.buffer.ReusedBufferedIndexInput;
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

public class HdfsDirectory extends Directory {

  private static final Log LOG = LogFactory.getLog(HdfsDirectory.class);

  private static AtomicLong deleteCounter = new AtomicLong();
  private static AtomicLong existsCounter = new AtomicLong();
  private static AtomicLong fileStatusCounter = new AtomicLong();
  private static AtomicLong renameCounter = new AtomicLong();
  private static AtomicLong listCounter = new AtomicLong();
  private static AtomicLong createCounter = new AtomicLong();
  private static AtomicLong isFileCounter = new AtomicLong();

  private static final boolean debug = false;

  static {
    if (debug) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            LOG.debug("Delete Counter [" + deleteCounter + "]");
            LOG.debug("Exists Counter [" + existsCounter + "]");
            LOG.debug("File Status Counter [" + fileStatusCounter + "]");
            LOG.debug("Rename Counter [" + renameCounter + "]");
            LOG.debug("List Counter [" + listCounter + "]");
            LOG.debug("Create Counter [" + createCounter + "]");
            LOG.debug("IsFile Counter [" + isFileCounter + "]");
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      });
      thread.setName("HDFS dir counter logger");
      thread.setDaemon(true);
      thread.start();
    }
  }

  private final Path path;
  private final FileSystem fileSystem;
  private final MetricsGroup metricsGroup;

  static class MetricsGroup {
    final Histogram readAccess;
    final Histogram writeAccess;
    final Meter writeThroughput;
    final Meter readThroughput;

    MetricsGroup(Histogram readAccess, Histogram writeAccess, Meter readThroughput, Meter writeThroughput) {
      this.readAccess = readAccess;
      this.writeAccess = writeAccess;
      this.readThroughput = readThroughput;
      this.writeThroughput = writeThroughput;
    }
  }

  /**
   * We keep the metrics separate per filesystem.
   */
  private static Map<URI, MetricsGroup> metricsGroupMap = new WeakHashMap<URI, MetricsGroup>();

  public HdfsDirectory(Configuration configuration, Path path) throws IOException {
    this.path = path;
    fileSystem = path.getFileSystem(configuration);
    setLockFactory(NoLockFactory.getNoLockFactory());
    synchronized (metricsGroupMap) {
      URI uri = fileSystem.getUri();
      MetricsGroup metricsGroup = metricsGroupMap.get(uri);
      if (metricsGroup == null) {
        String scope = uri.toString();

        Histogram readAccess = Metrics.newHistogram(new MetricName(ORG_APACHE_BLUR, HDFS, "Read Latency in \u00B5s",
            scope));
        Histogram writeAccess = Metrics.newHistogram(new MetricName(ORG_APACHE_BLUR, HDFS, "Write Latency in \u00B5s",
            scope));
        Meter readThroughput = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, HDFS, "Read Throughput", scope),
            "Read Bytes", TimeUnit.SECONDS);
        Meter writeThroughput = Metrics.newMeter(new MetricName(ORG_APACHE_BLUR, HDFS, "Write Throughput", scope),
            "Write Bytes", TimeUnit.SECONDS);
        metricsGroup = new MetricsGroup(readAccess, writeAccess, readThroughput, writeThroughput);
        metricsGroupMap.put(uri, metricsGroup);
      }
      this.metricsGroup = metricsGroup;
    }
  }

  @Override
  public String toString() {
    return "HdfsDirectory path=[" + path + "]";
  }

  public static class HdfsIndexInput extends ReusedBufferedIndexInput {

    private final long len;
    private FSDataInputStream inputStream;
    private boolean isClone;
    private final MetricsGroup metricsGroup;

    public HdfsIndexInput(FileSystem fileSystem, Path filePath, MetricsGroup metricsGroup) throws IOException {
      super(filePath.toString());
      inputStream = fileSystem.open(filePath);
      FileStatus fileStatus = fileSystem.getFileStatus(filePath);
      len = fileStatus.getLen();
      this.metricsGroup = metricsGroup;
    }

    @Override
    public long length() {
      return len;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {

    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
      synchronized (inputStream) {
        long start = System.nanoTime();
        inputStream.seek(getFilePointer());
        inputStream.readFully(b, offset, length);
        long end = System.nanoTime();
        metricsGroup.readAccess.update((end - start) / 1000);
        metricsGroup.readThroughput.mark(length);
      }
    }

    @Override
    protected void closeInternal() throws IOException {
      if (!isClone) {
        inputStream.close();
      }
    }

    @Override
    public ReusedBufferedIndexInput clone() {
      HdfsIndexInput clone = (HdfsIndexInput) super.clone();
      clone.isClone = true;
      return clone;
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    if (fileExists(name)) {
      throw new IOException("File [" + name + "] already exists found.");
    }
    final FSDataOutputStream outputStream = fileSystem.create(getPath(name));
    createCounter.incrementAndGet();
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
        metricsGroup.writeAccess.update((end - start) / 1000);
        metricsGroup.writeThroughput.mark(len);
      }

      @Override
      public void close() throws IOException {
        super.close();
        outputStream.close();
      }

      @Override
      public void seek(long pos) throws IOException {
        throw new IOException("seeks not allowed on IndexOutputs.");
      }
    };
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found.");
    }
    Path filePath = getPath(name);
    return new HdfsIndexInput(fileSystem, filePath, metricsGroup);
  }

  @Override
  public String[] listAll() throws IOException {
    listCounter.incrementAndGet();
    FileStatus[] files = fileSystem.listStatus(path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        try {
          isFileCounter.incrementAndGet();
          return fileSystem.isFile(path);
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
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    existsCounter.incrementAndGet();
    return fileSystem.exists(getPath(name));
  }

  @Override
  public void deleteFile(String name) throws IOException {
    if (fileExists(name)) {
      deleteCounter.incrementAndGet();
      fileSystem.delete(getPath(name), true);
    } else {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
  }

  @Override
  public long fileLength(String name) throws IOException {
    fileStatusCounter.incrementAndGet();
    FileStatus fileStatus = fileSystem.getFileStatus(getPath(name));
    return fileStatus.getLen();
  }

  @Override
  public void sync(Collection<String> names) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
  
  public Path getPath() {
    return path;
  }

  private Path getPath(String name) {
    return new Path(path, name);
  }

  public long getFileModified(String name) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
    Path file = getPath(name);
    fileStatusCounter.incrementAndGet();
    return fileSystem.getFileStatus(file).getModificationTime();
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    if (to instanceof DirectoryDecorator) {
      copy(((DirectoryDecorator) to).getOriginalDirectory(), src, dest, context);
    } else if (to instanceof HdfsDirectory) {
      if (quickMove(to, src, dest, context)) {
        return;
      }
    } else {
      super.copy(to, src, dest, context);
    }
  }

  private boolean quickMove(Directory to, String src, String dest, IOContext context) throws IOException {
    HdfsDirectory simpleTo = (HdfsDirectory) to;
    if (ifSameCluster(simpleTo, this)) {
      Path newDest = simpleTo.getPath(dest);
      Path oldSrc = getPath(src);
      renameCounter.incrementAndGet();
      return fileSystem.rename(oldSrc, newDest);
    }
    return false;
  }

  private boolean ifSameCluster(HdfsDirectory dest, HdfsDirectory src) {
    // @TODO
    return true;
  }

}
