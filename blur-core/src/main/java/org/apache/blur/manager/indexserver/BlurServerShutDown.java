package org.apache.blur.manager.indexserver;

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
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;


public class BlurServerShutDown implements Watcher {

  private static final Log LOG = LogFactory.getLog(BlurServerShutDown.class);

  public interface BlurShutdown {
    void shutdown();
  }

  private BlurShutdown shutdown;
  private ZooKeeper zooKeeper;

  public BlurServerShutDown() {
    Runtime runtime = Runtime.getRuntime();
    runtime.addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Closing zookeeper.");
          zooKeeper.close();
        } catch (InterruptedException e) {
          LOG.error("Unknown error while closing zookeeper.", e);
        }
      }
    }));
  }

  public void register(final BlurShutdown shutdown, ZooKeeper zooKeeper) {
    this.shutdown = shutdown;
    this.zooKeeper = zooKeeper;
    zooKeeper.register(new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        KeeperState state = event.getState();
        if (state == KeeperState.Expired) {
          LOG.fatal("Zookeeper session has [" + state + "] server process shutting down.");
          shutdown.shutdown();
        }
      }
    });
  }

  @Override
  public void process(WatchedEvent event) {
    register(shutdown, zooKeeper);
  }
}
