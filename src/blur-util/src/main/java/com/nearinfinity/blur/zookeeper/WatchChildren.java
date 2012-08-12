package com.nearinfinity.blur.zookeeper;

import java.io.Closeable;
import java.io.IOException;
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

  public static abstract class OnChange {
    public abstract void action(List<String> children);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    WatchChildren children = new WatchChildren("localhost", 30000, "/testing");
    children.watch(new OnChange() {
      @Override
      public void action(List<String> children) {
        System.out.println(children);
      }
    });

    Thread.sleep(100000000);
  }

  public WatchChildren(String connectString, int sessionTimeout, String path) throws IOException {
    this(new ZooKeeper(connectString, sessionTimeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    }), path);
  }

  public WatchChildren(ZooKeeper zooKeeper, String path) {
    _zooKeeper = zooKeeper;
    _path = path;
  }

  public WatchChildren watch(final OnChange onChange) {
    Thread thread = new Thread(new Runnable() {
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
              if (e.code() == Code.NONODE) {
                LOG.info("Path for watching not found [{0}], no longer watching.", _path);
                close();
                return;
              }
              LOG.error("Unknown error", e);
              throw new RuntimeException(e);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
        _running.set(false);
      }
    });
    thread.setName("Watching for children on path [" + _path + "] instance [" + instance + "]");
    thread.setDaemon(true);
    thread.start();
    return this;
  }
  
  private void startDoubleCheckThread() {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          try {
            Thread.sleep(_delay);
            if (!_running.get()) {
              return;
            }
            if (_zooKeeper.exists(_path, false) == null) {
              LOG.info("Path for watching not found [{0}], no longer double checking.", _path);
              return;
            }
            List<String> children = _zooKeeper.getChildren(_path, false);
            if (!isCorrect(children)) {
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
    thread.setName("Double check watching for node on path [" + _path + "]");
    thread.setDaemon(true);
    thread.start();
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
      _running.set(false);
      synchronized (_lock) {
        _lock.notify();
      }
    }
  }

}
