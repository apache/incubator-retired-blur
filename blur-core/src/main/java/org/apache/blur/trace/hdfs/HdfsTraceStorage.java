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
package org.apache.blur.trace.hdfs;

import static org.apache.blur.utils.BlurConstants.BLUR_HDFS_TRACE_PATH;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace.TraceId;
import org.apache.blur.trace.TraceCollector;
import org.apache.blur.trace.TraceStorage;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

public class HdfsTraceStorage extends TraceStorage {

  private final static Log LOG = LogFactory.getLog(HdfsTraceStorage.class);

  private Path _storePath;
  private BlockingQueue<TraceCollector> _queue = new LinkedBlockingQueue<TraceCollector>();
  private Thread _daemon;
  private FileSystem _fileSystem;

  public HdfsTraceStorage(BlurConfiguration configuration) throws IOException {
    super(configuration);
  }

  public void init(Configuration conf) throws IOException {
    _storePath = new Path(_configuration.get(BLUR_HDFS_TRACE_PATH));
    _fileSystem = _storePath.getFileSystem(conf);
    _fileSystem.mkdirs(_storePath);
    _daemon = new Thread(new Runnable() {
      @Override
      public void run() {
        Random random = new Random();
        while (true) {
          TraceCollector collector;
          try {
            collector = _queue.take();
          } catch (InterruptedException e) {
            return;
          }
          try {
            writeCollector(collector, random);
          } catch (Throwable t) {
            LOG.error("Unknown error while trying to write collector.", t);
          }
        }
      }
    });
    _daemon.setDaemon(true);
    _daemon.setName("ZooKeeper Trace Queue Writer");
    _daemon.start();
  }

  @Override
  public void store(TraceCollector collector) {
    try {
      _queue.put(collector);
    } catch (InterruptedException e) {
      LOG.error("Unknown error while trying to add collector on queue", e);
    }
  }

  private void writeCollector(TraceCollector collector, Random random) throws IOException {
    TraceId id = collector.getId();
    String storeId = id.getRootId();
    String requestId = id.getRequestId();
    if (requestId == null) {
      requestId = "";
    }
    Path tracePath = getTracePath(storeId);
    JSONObject jsonObject;
    try {
      jsonObject = collector.toJsonObject();
    } catch (JSONException e) {
      throw new IOException(e);
    }
    storeJson(new Path(tracePath, getRequestIdPathName(requestId, random)), jsonObject);
  }

  private String getRequestIdPathName(String requestId, Random random) {
    return requestId + "_" + random.nextLong();
  }

  public void storeJson(Path storePath, JSONObject jsonObject) throws IOException {
    FSDataOutputStream outputStream = _fileSystem.create(storePath, false);
    try {
      OutputStreamWriter writer = new OutputStreamWriter(outputStream);
      jsonObject.write(writer);
      writer.close();
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  private Path getTracePath(String traceId) {
    return new Path(_storePath, traceId);
  }

  @Override
  public void close() throws IOException {
    _fileSystem.close();
  }

  @Override
  public List<String> getTraceIds() throws IOException {
    FileStatus[] listStatus = _fileSystem.listStatus(_storePath);
    List<String> traceIds = new ArrayList<String>();
    for (FileStatus status : listStatus) {
      traceIds.add(status.getPath().getName());
    }
    return traceIds;
  }

  @Override
  public List<String> getRequestIds(String traceId) throws IOException {
    List<String> requestIds = new ArrayList<String>();
    Path tracePath = getTracePath(traceId);
    FileStatus[] listStatus = _fileSystem.listStatus(tracePath);
    for (FileStatus status : listStatus) {
      String name = status.getPath().getName();
      int indexOf = name.lastIndexOf('_');
      if (indexOf > 0) {
        requestIds.add(name.substring(0, indexOf));
      }
    }
    return requestIds;
  }

  @Override
  public String getRequestContentsJson(String traceId, String requestId) throws IOException {
    Path path = findPath(traceId, requestId);
    FSDataInputStream inputStream = _fileSystem.open(path);
    try {
      return IOUtils.toString(inputStream);
    } finally {
      inputStream.close();
    }
  }

  private Path findPath(String traceId, String requestId) throws IOException {
    Path tracePath = getTracePath(traceId);
    if (!_fileSystem.exists(tracePath)) {
      throw new IOException("Trace [" + traceId + "] not found.");
    }
    FileStatus[] listStatus = _fileSystem.listStatus(tracePath);
    for (FileStatus status : listStatus) {
      Path path = status.getPath();
      String name = path.getName();
      int indexOf = name.lastIndexOf('_');
      if (indexOf > 0) {
        if (name.substring(0, indexOf).equals(requestId)) {
          return path;
        }
      }
    }
    throw new IOException("Request [" + requestId + "] not found.");
  }

  @Override
  public void removeTrace(String traceId) throws IOException {
    Path storePath = getTracePath(traceId);
    _fileSystem.delete(storePath, true);
  }

}
