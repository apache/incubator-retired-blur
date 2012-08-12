package com.nearinfinity.blur.zookeeper;

import java.io.Closeable;
import java.io.IOException;
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

public class WatchNodeExistance implements Closeable {

  private final static Log LOG = LogFactory.getLog(WatchNodeExistance.class);
  private final ZooKeeper _zooKeeper;
  private final String _path;
  private final Object _lock = new Object();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private long _delay = TimeUnit.SECONDS.toMillis(3);
  private Stat _stat;
  private final String instance = UUID.randomUUID().toString();
  private Thread _doubleCheckThread;
  private Thread _watchThread;

  public static abstract class OnChange {
    public abstract void action(Stat stat);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    WatchNodeExistance children = new WatchNodeExistance("localhost", 30000, "/testing");
    children.watch(new OnChange() {
      @Override
      public void action(Stat stat) {
        System.out.println(stat);
      }
    });

    Thread.sleep(100000000);
  }

  public WatchNodeExistance(String connectString, int sessionTimeout, String path) throws IOException {
    _zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });
    _path = path;
  }

  public WatchNodeExistance(ZooKeeper zooKeeper, String path) {
    _zooKeeper = zooKeeper;
    _path = path;
  }

  public WatchNodeExistance watch(final OnChange onChange) {
    _watchThread = new Thread(new Runnable() {
      @Override
      public void run() {
        startDoubleCheckThread();
        while (_running.get()) {
          synchronized (_lock) {
            try {
              Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_lock) {
                    _lock.notify();
                  }
                }
              };
              _stat = _zooKeeper.exists(_path, watcher);
              onChange.action(_stat);
              _lock.wait();
            } catch (KeeperException e) {
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
            if (!isCorrect(stat)) {
              LOG.error("Double check triggered for [" + _path + "]");
              synchronized (_lock) {
                _lock.notify();
              }
            }
          } catch (KeeperException e) {
            LOG.error("Unknown error", e);
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _doubleCheckThread.setName("Double check watching for node on path [" + _path + "]");
    _doubleCheckThread.setDaemon(true);
    _doubleCheckThread.start();
  }

  protected boolean isCorrect(Stat stat) {
    if (stat == null && _stat == null) {
      return true;
    }
    if (stat == null || _stat == null) {
      return false;
    }
    return stat.equals(_stat);
  }

  public void close() {
    if (_running.get()) {
      _running.set(false);
      _doubleCheckThread.interrupt();
      _watchThread.interrupt();
      synchronized (_lock) {
        _lock.notify();
      }
    }
  }

}
