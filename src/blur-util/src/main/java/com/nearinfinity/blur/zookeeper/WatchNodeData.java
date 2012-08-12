package com.nearinfinity.blur.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class WatchNodeData implements Closeable {

  private final static Log LOG = LogFactory.getLog(WatchNodeData.class);
  private final ZooKeeper _zooKeeper;
  private final String _path;
  private final Object _lock = new Object();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private long _delay = TimeUnit.SECONDS.toMillis(3);
  private byte[] _data;
  private final String instance = UUID.randomUUID().toString();
  private Thread _doubleCheckThread;
  private Thread _watchThread;

  public static abstract class OnChange {
    public abstract void action(byte[] data);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    WatchNodeData children = new WatchNodeData("localhost", 30000, "/testing");
    children.watch(new OnChange() {
      @Override
      public void action(byte[] data) {
        System.out.println(new String(data));
      }
    });

    Thread.sleep(100000000);
  }

  public WatchNodeData(String connectString, int sessionTimeout, String path) throws IOException {
    _zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    _path = path;
  }

  public WatchNodeData(ZooKeeper zooKeeper, String path) {
    _zooKeeper = zooKeeper;
    _path = path;
    LOG.info("Creating watch [{0}]", instance);
  }

  public WatchNodeData watch(final OnChange onChange) {
    _watchThread = new Thread(new Runnable() {

      @Override
      public void run() {
        startDoubleCheckThread();
        while (_running.get()) {
          synchronized (_lock) {
            try {
              Stat stat = _zooKeeper.exists(_path, false);
              if (stat == null) {
                LOG.info("Path [{0}] not found.", _path);
                return;
              }
              Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_lock) {
                    _lock.notify();
                  }
                }
              };
              _data = _zooKeeper.getData(_path, watcher, stat);
              onChange.action(_data);
              _lock.wait();
            } catch (KeeperException e) {
              if (!_running.get()) {
                LOG.info("Error [{0}]", e.getMessage());
                return;
              }
              LOG.error("Unknown error", e);
              throw new RuntimeException(e);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    });
    _watchThread.setName("Watching for node on path [" + _path + "] instance [" + instance + "]");
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
            Stat stat = _zooKeeper.exists(_path, false);
            if (stat == null) {
              LOG.info("Path [{0}] not found.", _path);
              LOG.error("Double check triggered for [" + _path + "]");
              synchronized (_lock) {
                _lock.notify();
              }
              return;
            }

            byte[] data = _zooKeeper.getData(_path, false, stat);
            if (!isCorrect(data)) {
              LOG.error("Double check triggered for [" + _path + "]");
              synchronized (_lock) {
                _lock.notify();
              }
            }
          } catch (KeeperException e) {
            if (!_running.get()) {
              LOG.info("Error [{0}]", e.getMessage());
              return;
            }
            LOG.error("Unknown error [{0}]", e, instance);
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _doubleCheckThread.setName("Double check watching for path [" + _path + "]");
    _doubleCheckThread.setDaemon(true);
    _doubleCheckThread.start();
  }

  protected boolean isCorrect(byte[] data) {
    if (data == null && _data == null) {
      return true;
    }
    if (data == null || _data == null) {
      return false;
    }
    return Arrays.equals(data, _data);
  }

  public void close() {
    if (_running.get()) {
      LOG.warn("Closing [{0}]", instance);
      _running.set(false);
      _doubleCheckThread.interrupt();
      _watchThread.interrupt();
      synchronized (_lock) {
        _lock.notify();
      }
    }
  }

}
