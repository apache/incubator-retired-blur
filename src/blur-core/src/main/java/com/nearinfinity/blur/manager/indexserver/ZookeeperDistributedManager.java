package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZookeeperDistributedManager extends DistributedManager {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperDistributedManager.class);
    
    private ZooKeeper zooKeeper;

    @Override
    public void close() {
        if (zooKeeper != null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                LOG.error("Error while closing zookeeper",e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void createEphemeralPathInternal(String path) {
        try {
            zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("Error while creating ephemeral path [" + path + "]",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void createPathInternal(String path) {
        try {
            zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOG.error("Error while creating ephemeral path [" + path + "]",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean existsInternal(String path) {
        Stat stat;
        try {
            stat = zooKeeper.exists(path, false);
        } catch (Exception e) {
            LOG.error("Error while checking path [" + path + "] for existance",e);
            throw new RuntimeException(e);
        }
        if (stat == null) {
            return false;
        }
        return true;
    }

    @Override
    protected List<String> listInternal(String path) {
        try {
            return new ArrayList<String>(zooKeeper.getChildren(path, false));
        } catch (Exception e) {
            LOG.error("Error while get a listing for path [" + path + "]",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void registerCallableOnChangeInternal(final Runnable runnable, String path) {
        try {
            zooKeeper.getChildren(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    runnable.run();
                }
            });
        } catch (Exception e) {
            LOG.error("Error while adding a watcher for path [" + path + "]",e);
            throw new RuntimeException(e);
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}
