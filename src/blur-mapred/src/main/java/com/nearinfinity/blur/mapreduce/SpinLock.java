package com.nearinfinity.blur.mapreduce;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class SpinLock {

  private static final Log LOG = LogFactory.getLog(SpinLock.class);
  private ZooKeeper _zooKeeper;
  private String _path;
  private int _maxCopies;
  private String _name;
  private long _delay = TimeUnit.SECONDS.toMillis(30);
  private Progressable _progressable;

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        System.out.println("go");
      }
    };
    String zkConnectionStr = "localhost";
    SpinLock lock = new SpinLock(progressable, zkConnectionStr, "test", "/test-spin-lock");
    lock.copyLock(null);
  }

  public SpinLock(Progressable progressable, String zkConnectionStr, String name, String path) throws IOException, KeeperException, InterruptedException {
    _path = path;
    _name = name;
    _progressable = progressable;
    _zooKeeper = new ZooKeeper(zkConnectionStr, 60000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    checkMaxCopies();
  }

  private void checkMaxCopies() throws KeeperException, InterruptedException {
    Stat stat = _zooKeeper.exists(_path, false);
    if (stat == null) {
      LOG.warn("Path [{0}] not set no limit on copies.", _path);
      _maxCopies = Integer.MAX_VALUE;
    } else {
      byte[] data = _zooKeeper.getData(_path, false, stat);
      _maxCopies = Integer.parseInt(new String(data));
    }
  }

  public void copyLock(@SuppressWarnings("rawtypes") Context context) {
    if (_maxCopies == Integer.MAX_VALUE) {
      return;
    }
    try {
      String newpath = _zooKeeper.create(_path + "/" + _name, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      while (true) {
        _progressable.progress();
        checkMaxCopies();
        List<String> children = new ArrayList<String>(_zooKeeper.getChildren(_path, false));
        Collections.sort(children);
        for (int i = 0; i < _maxCopies && i < children.size(); i++) {
          if (newpath.equals(_path + "/" + children.get(i))) {
            return;
          }
        }
        LOG.info("Waiting for copy lock");
        context.setStatus("Waiting for copy lock");
        Thread.sleep(_delay);
      }
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

  }

}
