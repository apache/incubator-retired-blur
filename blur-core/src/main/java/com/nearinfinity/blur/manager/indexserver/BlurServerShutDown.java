package com.nearinfinity.blur.manager.indexserver;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class BlurServerShutDown implements Watcher, ZookeeperPathConstants {
    
    private static final Log LOG = LogFactory.getLog(BlurServerShutDown.class);

    public interface BlurShutdown {
        void shutdown();
    }

    private BlurShutdown shutdown;
    private ZooKeeper zooKeeper;
    
    public void register(final BlurShutdown shutdown, ZooKeeper zooKeeper) {
        this.shutdown = shutdown;
        this.zooKeeper = zooKeeper;
        zooKeeper.register(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                KeeperState state = event.getState();
                if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
                    LOG.fatal("Zookeeper session has [" + state + "] server process shutting down.");
                    shutdown.shutdown();
                }
            }
        });
        registerShutdownEvent(shutdown, zooKeeper);
    }
    
    private void registerShutdownEvent(BlurShutdown shutdown, ZooKeeper zooKeeper) {
        try {
            if (zooKeeper.exists(BLUR_SAFEMODE_SHUTDOWN, this) != null) {
                LOG.info("Shutdown flag detected, server process shutting down.");
                shutdown.shutdown();
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        register(shutdown, zooKeeper);
    }
}
