package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.store.lock.ZkUtils;

public class ZookeeperDistributedManager extends DistributedManager {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperDistributedManager.class);
    
    private ZooKeeper zooKeeper;

    @Override
    public void close() {
        //do nothing, zooKeeper can close itself
    }
    
    @Override
    protected boolean saveDataInternal(byte[] data, String path) {
        try {
            Stat stat = zooKeeper.exists(path, false);
            zooKeeper.setData(path, data, stat.getVersion());
            return true;
        } catch (Exception e) {
            if (e instanceof KeeperException) {
                KeeperException exception = (KeeperException) e;
                if (exception.code() == Code.BADVERSION) {
                    return false;
                }
            }
            LOG.error("Error while set data into path [{0}]",e,path);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void fetchDataInternal(Value value, String path) {
        try {
            Stat stat = new Stat();
            value.data = zooKeeper.getData(path, false, stat);
            value.version = stat.getVersion();
        } catch (Exception e) {
            LOG.error("Error while fetching data from path [{0}]",e,path);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void createEphemeralPathInternal(String path) {
        try {
            zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("Error while creating ephemeral path [{0}]",e,path);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void createPathInternal(String path) {
        try {
            ZkUtils.mkNodesStr(zooKeeper, path);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof KeeperException) {
                KeeperException exception = (KeeperException) cause;
                if (exception.code() == Code.NODEEXISTS) {
                    return;
                }
            }
            LOG.error("Error while creating ephemeral path [{0}]",e,path);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean existsInternal(String path) {
        Stat stat;
        try {
            stat = zooKeeper.exists(path, false);
        } catch (Exception e) {
            LOG.error("Error while checking path [{0}] for existance",e,path);
            throw new RuntimeException(e);
        }
        return stat != null;
    }

    @Override
    protected List<String> listInternal(String path) {
        try {
            return new ArrayList<String>(zooKeeper.getChildren(path, false));
        } catch (Exception e) {
            LOG.error("Error while get a listing for path [{0}]",e,path);
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
            LOG.error("Error while adding a watcher for path [{0}]",e,path);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void removeEphemeralPathOnShutdownInternal(final String path) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    zooKeeper.delete(path, -1);
                    zooKeeper.close();
                } catch (Exception e) {
                    LOG.error("Error while deleting path [{0}] during shutdown.",e,path);
                }
            }
        }));
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    protected void lockInternal(String path) {
        LOG.info("Attempting to obtain lock [{0}]",path);
        while (true) {
            try {
                zooKeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return;
            } catch (KeeperException e) {
                if (e.code() == Code.NODEEXISTS) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    continue;
                }
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void unlockInternal(String path) {
        LOG.info("Removing lock [{0}]",path);
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                return;
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void removePath(String path) {
        try {
            zooKeeper.delete(path, -1);
        } catch (KeeperException e) {
            if (e.code() == Code.NONODE) {
                return;
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
