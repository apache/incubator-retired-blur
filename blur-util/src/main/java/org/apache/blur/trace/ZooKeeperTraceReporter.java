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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace.TraceId;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperTraceReporter extends TraceReporter {

  private final static Log LOG = LogFactory.getLog(ZooKeeperTraceReporter.class);

  private final String _zkConnectionStr;
  private final ZooKeeperClient _zooKeeperClient;
  private final String _storePath;
  private final BlockingQueue<TraceCollector> _queue = new LinkedBlockingQueue<TraceCollector>();
  private final Thread _daemon;

  public ZooKeeperTraceReporter(BlurConfiguration configuration) throws IOException {
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
    _daemon.setName("ZooKeeper Trace Queue Writer");
    _daemon.start();
  }

  @Override
  public void report(TraceCollector collector) {
    try {
      _queue.put(collector);
    } catch (InterruptedException e) {
      LOG.error("Unknown error while trying to add collector on queue", e);
    }
  }

  private void writeCollector(TraceCollector collector) {
    TraceId id = collector.getId();
    String storeId = id.getRootId();
    String storePath = getStorePath(storeId);
    createIfMissing(storePath);
    String json = collector.toJson();
    try {
      _zooKeeperClient.create(storePath + "/trace-", json.getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE,
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

}
