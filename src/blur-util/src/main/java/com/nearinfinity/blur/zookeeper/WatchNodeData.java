package com.nearinfinity.blur.zookeeper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class WatchNodeData {

  private final static Log LOG = LogFactory.getLog(WatchNodeData.class);
  private final ZooKeeper _zooKeeper;
  private final String _path;
  private final Object _lock = new Object();
  private final AtomicBoolean _running = new AtomicBoolean(true);

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
  }

  public WatchNodeData watch(final OnChange onChange) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          synchronized (_lock) {
            try {
              Stat stat = _zooKeeper.exists(_path, false);
              if (stat == null) {
                LOG.info("Path [{0}] not found.", _path);
                return;
              }
              onChange.action(_zooKeeper.getData(_path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (_lock) {
                    _lock.notify();
                  }
                }
              }, stat));
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
    thread.setName("Watching for node on path [" + _path + "]");
    thread.setDaemon(true);
    thread.start();
    return this;
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
