package org.apache.blur.zookeeper;

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
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperClient extends ZooKeeper {

  private static final Log LOG = LogFactory.getLog(ZooKeeperClient.class);
  private final int internalSessionTimeout;

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
      throws IOException {
    super(connectString, sessionTimeout, watcher, canBeReadOnly);
    internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
      byte[] sessionPasswd, boolean canBeReadOnly) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly);
    internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd)
      throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
    internalSessionTimeout = sessionTimeout;
  }

  static abstract class ZKExecutor<T> {
    String _name;

    ZKExecutor(String name) {
      _name = name;
    }

    abstract T execute() throws KeeperException, InterruptedException;
  }

  public <T> T execute(ZKExecutor<T> executor) throws KeeperException, InterruptedException {
    final long timestmap = System.currentTimeMillis();
    int sessionTimeout = getSessionTimeout();
    if (sessionTimeout == 0) {
      sessionTimeout = internalSessionTimeout;
    }
    while (true) {
      Tracer trace = Trace.trace("remote call - zookeeper", Trace.param("method", executor._name),
          Trace.param("toString", executor.toString()));
      try {
        return executor.execute();
      } catch (KeeperException e) {
        if (e.code() == Code.CONNECTIONLOSS && timestmap + sessionTimeout >= System.currentTimeMillis()) {
          LOG.warn("Connection loss");
          ZkUtils.pause(this);
          continue;
        }
        throw e;
      } finally {
        trace.done();
      }
    }
  }

  @Override
  public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<String>("create") {
      @Override
      String execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - create [{0}] [{1}] [{2}] [{3}]", path, data, acl, createMode);
        return ZooKeeperClient.super.create(path, data, acl, createMode);
      }
    });
  }

  @Override
  public void delete(final String path, final int version) throws InterruptedException, KeeperException {
    execute(new ZKExecutor<Void>("delete") {
      @Override
      Void execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - delete [{0}] [{1}]", path, version);
        ZooKeeperClient.super.delete(path, version);
        return null;
      }
    });
  }

  @Override
  public List<OpResult> multi(final Iterable<Op> ops) throws InterruptedException, KeeperException {
    return execute(new ZKExecutor<List<OpResult>>("multi") {
      @Override
      List<OpResult> execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.multi(ops);
      }
    });
  }

  @Override
  public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("exists") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - exists [{0}] [{1}]", path, watcher);
        return ZooKeeperClient.super.exists(path, watcher);
      }
    });
  }

  @Override
  public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("exists") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - exists [{0}] [{1}]", path, watch);
        return ZooKeeperClient.super.exists(path, watch);
      }
    });
  }

  @Override
  public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<byte[]>("getData") {
      @Override
      byte[] execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getData [{0}] [{1}] [{2}]", path, watcher, stat);
        return ZooKeeperClient.super.getData(path, watcher, stat);
      }
    });
  }

  @Override
  public byte[] getData(final String path, final boolean watch, final Stat stat) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<byte[]>("getData") {
      @Override
      byte[] execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getData [{0}] [{1}] [{2}]", path, watch, stat);
        return ZooKeeperClient.super.getData(path, watch, stat);
      }
    });
  }

  @Override
  public Stat setData(final String path, final byte[] data, final int version) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<Stat>("setData") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - setData [{0}] [{1}] [{2}]", path, data, version);
        return ZooKeeperClient.super.setData(path, data, version);
      }
    });
  }

  @Override
  public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<ACL>>("getACL") {
      @Override
      List<ACL> execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.getACL(path, stat);
      }
    });
  }

  @Override
  public Stat setACL(final String path, final List<ACL> acl, final int version) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<Stat>("setACL") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.setACL(path, acl, version);
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final Watcher watcher) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getChildren [{0}] [{1}]", path, watcher);
        return ZooKeeperClient.super.getChildren(path, watcher);
      }

      @Override
      public String toString() {
        return "path=" + path + " watcher=" + watcher;
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getChildren [{0}] [{1}]", path, watch);
        return ZooKeeperClient.super.getChildren(path, watch);
      }

      @Override
      public String toString() {
        return "path=" + path + " watch=" + watch;
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final Watcher watcher, final Stat stat) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getChildren [{0}] [{1}] [{2}]", path, watcher, stat);
        return ZooKeeperClient.super.getChildren(path, watcher, stat);
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final boolean watch, final Stat stat) throws KeeperException,
      InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOG.debug("ZK Call - getChildren [{0}] [{1}] [{2}]", path, watch, stat);
        return ZooKeeperClient.super.getChildren(path, watch, stat);
      }
    });
  }

}
