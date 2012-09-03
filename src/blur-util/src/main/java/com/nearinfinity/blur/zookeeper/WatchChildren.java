package com.nearinfinity.blur.zookeeper;

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
import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class WatchChildren implements Closeable {

  private final static Log LOG = LogFactory.getLog(WatchChildren.class);
  private final ZooKeeper _zooKeeper;
  private final String _path;
  private final Object _lock = new Object();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private long _delay = TimeUnit.SECONDS.toMillis(3);
  private List<String> _children;
  private final String instance = UUID.randomUUID().toString();
  private Thread _doubleCheckThread;
  private Thread _watchThread;
  private final boolean _debug = true;
  private String _debugStackTrace;

  public static abstract class OnChange {
    public abstract void action(List<String> children);
  }

  public WatchChildren(ZooKeeper zooKeeper, String path) {
    _zooKeeper = zooKeeper;
    _path = path;
    LOG.debug("Creating watch [{0}]", instance);
  }

  public WatchChildren watch(final OnChange onChange) {
    if (_debug) {
      StringWriter writer = new StringWriter();
      PrintWriter printWriter = new PrintWriter(writer);
      new Throwable().printStackTrace(printWriter);
      printWriter.close();
      _debugStackTrace = writer.toString();
    }
    _watchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        startDoubleCheckThread();
        while (_running.get()) {
          synchronized (_lock) {
            try {
              _children = _zooKeeper.getChildren(_path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_lock) {
                    _lock.notify();
                  }
                }
              });
              onChange.action(_children);
              _lock.wait();
            } catch (KeeperException e) {
              LOG.error("Error in instance [{0}]", e, instance);
              if (!_running.get()) {
                LOG.info("Error [{0}]", e.getMessage());
                return;
              }
              if (e.code() == Code.NONODE) {
                if (_debug) {
                  LOG.debug("Path for watching not found [{0}], no longer watching, debug [{1}].", _path, _debugStackTrace);
                } else {
                  LOG.debug("Path for watching not found [{0}], no longer watching.", _path);
                }
                close();
                return;
              }
              if (_debug) {
                LOG.error("Unknown error [{0}]", e, _debugStackTrace);
              } else {
                LOG.error("Unknown error", e);
              }
              throw new RuntimeException(e);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
        _running.set(false);
      }
    });
    _watchThread.setName("Watch Children [" + _path + "][" + instance + "]");
    _watchThread.setDaemon(true);
    _watchThread.start();
    return this;
  }

  private void startDoubleCheckThread() {
    _doubleCheckThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          try {
            Thread.sleep(_delay);
            if (!_running.get()) {
              return;
            }
            if (_zooKeeper.exists(_path, false) == null) {
              LOG.debug("Path for watching not found [{0}], no longer double checking.", _path);
              return;
            }
            List<String> children = _zooKeeper.getChildren(_path, false);
            if (!isCorrect(children)) {
              LOG.error("Double check triggered for [" + _path + "] [" + instance + "]");
              synchronized (_lock) {
                _lock.notify();
              }
            }
          } catch (KeeperException e) {
            if (!_running.get()) {
              LOG.info("Error [{0}]", e.getMessage());
              return;
            }
            if (e.code() == Code.SESSIONEXPIRED) {
              LOG.warn("Session expired for [" + _path + "] [" + instance + "]");
              return;
            }
            LOG.error("Unknown error", e);
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _doubleCheckThread.setName("Poll Watch Children [" + _path + "][" + instance + "]");
    _doubleCheckThread.setDaemon(true);
    _doubleCheckThread.start();
  }

  protected boolean isCorrect(List<String> children) {
    if (children == null && _children == null) {
      return true;
    }
    if (children == null || _children == null) {
      return false;
    }
    return children.equals(_children);
  }

  public void close() {
    if (_running.get()) {
      LOG.warn("Closing [{0}]", instance);
      _running.set(false);
      if (_doubleCheckThread != null) {
        _doubleCheckThread.interrupt();
      }
      if (_watchThread != null) {
        _watchThread.interrupt();
      }
    }
  }
}
