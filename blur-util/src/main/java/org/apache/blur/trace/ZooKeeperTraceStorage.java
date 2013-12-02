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
package org.apache.blur.trace;

import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static org.apache.blur.utils.BlurConstants.BLUR_ZOOKEEPER_TRACE_PATH;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace.TraceId;
import org.apache.blur.zookeeper.ZkUtils;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperTraceStorage extends TraceStorage {

  private static final String UTF_8 = "UTF-8";

  private static final String TRACE = "trace_";

  private final static Log LOG = LogFactory.getLog(ZooKeeperTraceStorage.class);

  private final String _zkConnectionStr;
  private final ZooKeeperClient _zooKeeperClient;
  private final String _storePath;
  private final BlockingQueue<TraceCollector> _queue = new LinkedBlockingQueue<TraceCollector>();
  private final Thread _daemon;

  public ZooKeeperTraceStorage(BlurConfiguration configuration) throws IOException {
    super(configuration);
    _zkConnectionStr = configuration.get(BLUR_ZOOKEEPER_CONNECTION);
    _storePath = configuration.get(BLUR_ZOOKEEPER_TRACE_PATH);
    _zooKeeperClient = new ZooKeeperClient(_zkConnectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    createIfMissing(_storePath);
    _daemon = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          TraceCollector collector;
          try {
            collector = _queue.take();
          } catch (InterruptedException e) {
            return;
          }
          try {
            writeCollector(collector);
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

  private void writeCollector(TraceCollector collector) {
    TraceId id = collector.getId();
    String storeId = id.getRootId();
    String requestId = id.getRequestId();
    if (requestId == null) {
      requestId = "";
    }
    String storePath = getStorePath(storeId);
    String json = collector.toJson();
    storeJson(storePath, requestId, json);
  }

  public void storeJson(String storePath, String requestId, String json) {
    try {
      createIfMissing(storePath);
      _zooKeeperClient.create(storePath + "/" + TRACE + requestId + "_", json.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void createIfMissing(String storePath) {
    try {
      Stat stat = _zooKeeperClient.exists(storePath, false);
      if (stat == null) {
        _zooKeeperClient.create(storePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      if (e.code() == Code.NODEEXISTS) {
        return;
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private String getStorePath(String storeId) {
    return _storePath + "/" + storeId;
  }

  @Override
  public void close() throws IOException {
    try {
      _zooKeeperClient.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getTraceIds() throws IOException {
    try {
      return _zooKeeperClient.getChildren(_storePath, false);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<String> getRequestIds(String traceId) throws IOException {
    String storePath = getStorePath(traceId);
    try {
      Stat stats = _zooKeeperClient.exists(storePath, false);
      if (stats == null) {
        throw new IOException("Trace [" + traceId + "] not found.");
      }
      List<String> children = _zooKeeperClient.getChildren(storePath, false);
      List<String> requestIds = new ArrayList<String>();
      for (String c : children) {
        String requestId = getRequestId(c);
        if (requestId != null) {
          requestIds.add(requestId);
        }
      }
      return requestIds;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getRequestId(String s) {
    if (s.startsWith(TRACE)) {
      int lastIndexOf = s.lastIndexOf('_');
      return s.substring(TRACE.length(), lastIndexOf);
    }
    return null;
  }

  @Override
  public String getRequestContentsJson(String traceId, String requestId) throws IOException {
    String storePath = getStorePath(traceId);
    try {
      Stat stats = _zooKeeperClient.exists(storePath, false);
      if (stats == null) {
        throw new IOException("Trace [" + traceId + "] not found.");
      }
      String requestPath = getRequestStorePath(storePath, requestId);
      if (requestPath == null) {
        throw new IOException("Request [" + requestId + "] not found.");
      }
      Stat dataStat = _zooKeeperClient.exists(requestPath, false);
      byte[] data = _zooKeeperClient.getData(requestPath, false, dataStat);
      return new String(data, UTF_8);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getRequestStorePath(String storePath, String requestId) throws KeeperException, InterruptedException {
    List<String> children = _zooKeeperClient.getChildren(storePath, false);
    for (String c : children) {
      if (c.startsWith(TRACE + requestId + "_")) {
        return storePath + "/" + c;
      }
    }
    return null;
  }

  @Override
  public void removeTrace(String traceId) throws IOException {
    String storePath = getStorePath(traceId);
    try {
      ZkUtils.rmr(_zooKeeperClient, storePath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
