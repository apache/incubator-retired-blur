package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ZookeeperManagedDistributedIndexServer extends DistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperManagedDistributedIndexServer.class);
    private static final String BLUR_BASE_PATH = "/blur";
    private static final String BLUR_REGISTERED_SHARDS_PATH = "/blur/shard-nodes";
    private static final String BLUR_ONLINE_SHARDS_PATH = "/blur/online/shard-nodes";
    private static final String BLUR_ONLINE_CONTROLLERS_PATH = "/blur/online/controller-nodes";
    
    private Zk zk;

    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private Timer daemon;
    private long zkPollDelay = TimeUnit.MINUTES.toMillis(1);
    
    @Override
    public ZookeeperManagedDistributedIndexServer init() {
        super.init();
        setupZookeeper();
        registerMyself();
        waitIfInSafeMode();
        startPollingDaemon();
        pollForState();
        return this;
    }

    private void waitIfInSafeMode() {
        //todo
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void registerMyself() {
        zk.createEphemeralPath(BLUR_ONLINE_SHARDS_PATH,getNodeName());
    }

    private synchronized void pollForState() {
        List<String> shardNodes = zk.list(BLUR_REGISTERED_SHARDS_PATH);
        List<String> onlineShardNodes = zk.list(BLUR_ONLINE_SHARDS_PATH);
        controllers = zk.list(BLUR_ONLINE_CONTROLLERS_PATH);
        List<String> offlineShardNodes = new ArrayList<String>(shardNodes);
        offlineShardNodes.removeAll(onlineShardNodes);
        boolean stateChange = false;
        if (!shardNodes.equals(shards)) {
            LOG.info("Shard servers in the cluster changed from [" + shards +
                    "] to [" + shardNodes + "]");
            stateChange = true;
            shards = shardNodes;
        }
        if (!offlineShardNodes.equals(offlineShards)) {
            LOG.info("Offline shard servers changed from [" + offlineShards +
            		"] to [" + offlineShardNodes + "]");
            stateChange = true;
            offlineShards = offlineShardNodes;
        }
        if (stateChange) {
            shardServerStateChange();
        }
        register(BLUR_REGISTERED_SHARDS_PATH);
        register(BLUR_ONLINE_CONTROLLERS_PATH);
        register(BLUR_ONLINE_SHARDS_PATH);
    }

    private void register(String path) {
        zk.registerCallableOnChange(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                pollForState();
                return null;
            }
        },path);
    }

    @Override
    public void close() {
        super.close();
        daemon.cancel();
        daemon.purge();
        zk.close();
    }

    @Override
    public List<String> getControllerServerList() {
        return controllers;
    }

    @Override
    public List<String> getOfflineShardServers() {
        return offlineShards;
    }

    @Override
    public List<String> getShardServerList() {
        return shards;
    }
    
    public Zk getZk() {
        return zk;
    }

    public ZookeeperManagedDistributedIndexServer setZk(Zk zk) {
        this.zk = zk;
        return this;
    }
    
    public long getZkPollDelay() {
        return zkPollDelay;
    }

    public ZookeeperManagedDistributedIndexServer setZkPollDelay(long zkPollDelay) {
        this.zkPollDelay = zkPollDelay;
        return this;
    }

    private void startPollingDaemon() {
        daemon = new Timer("Zookeeper-Polling-Timer", true);
        daemon.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                pollForState();
            }
        }, zkPollDelay, zkPollDelay);
    }
    
    private void setupZookeeper() {
        if (!zk.exists(BLUR_BASE_PATH)) {
            zk.createPath(BLUR_BASE_PATH);
        }
        if (!zk.exists(BLUR_REGISTERED_SHARDS_PATH)) {
            zk.createPath(BLUR_REGISTERED_SHARDS_PATH);
        }
        if (!zk.exists(BLUR_ONLINE_SHARDS_PATH)) {
            zk.createPath(BLUR_ONLINE_SHARDS_PATH);
        }
        if (!zk.exists(BLUR_ONLINE_CONTROLLERS_PATH)) {
            zk.createPath(BLUR_ONLINE_CONTROLLERS_PATH);
        }
    }
}
