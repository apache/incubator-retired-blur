package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ManagedDistributedIndexServer extends DistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(ManagedDistributedIndexServer.class);
    private static final String BLUR_BASE_PATH = "/blur";
    private static final String BLUR_REGISTERED_SHARDS_PATH = "/blur/shard-nodes";
    private static final String BLUR_ONLINE_PATH = "/blur/online";
    private static final String BLUR_ONLINE_SHARDS_PATH = "/blur/online/shard-nodes";
    private static final String BLUR_ONLINE_CONTROLLERS_PATH = "/blur/online/controller-nodes";
    
    private DistributedManager dm;

    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private List<String> onlineShards = new ArrayList<String>();
    private Timer daemon;
    private long zkPollDelay = TimeUnit.MINUTES.toMillis(1);
    
    
    @Override
    public ManagedDistributedIndexServer init() {
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
        if (!dm.exists(BLUR_REGISTERED_SHARDS_PATH,getNodeName())) {
            dm.createPath(BLUR_REGISTERED_SHARDS_PATH,getNodeName());
        }
        while (dm.exists(BLUR_ONLINE_SHARDS_PATH,getNodeName())) {
            LOG.info("Waiting to register myself [" + getNodeName() + "].");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        dm.createEphemeralPath(BLUR_ONLINE_SHARDS_PATH,getNodeName());
        LOG.info("Registered [" + getNodeName() + "].");
    }

    private synchronized void pollForState() {
        List<String> shardNodes = dm.list(BLUR_REGISTERED_SHARDS_PATH);
        onlineShards = dm.list(BLUR_ONLINE_SHARDS_PATH);
        controllers = dm.list(BLUR_ONLINE_CONTROLLERS_PATH);
        List<String> offlineShardNodes = new ArrayList<String>(shardNodes);
        offlineShardNodes.removeAll(onlineShards);
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
        dm.registerCallableOnChange(new Runnable() {
            @Override
            public void run() {
                pollForState();
            }
        },path);
    }

    @Override
    public void close() {
        super.close();
        daemon.cancel();
        daemon.purge();
        dm.close();
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
    public List<String> getOnlineShardServers() {
        return onlineShards;
    }

    @Override
    public List<String> getShardServerList() {
        return shards;
    }
    
    public DistributedManager getZk() {
        return dm;
    }

    public ManagedDistributedIndexServer setZk(DistributedManager zk) {
        this.dm = zk;
        return this;
    }
    
    public long getZkPollDelay() {
        return zkPollDelay;
    }

    public ManagedDistributedIndexServer setZkPollDelay(long zkPollDelay) {
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
        if (!dm.exists(BLUR_BASE_PATH)) {
            dm.createPath(BLUR_BASE_PATH);
        }
        if (!dm.exists(BLUR_REGISTERED_SHARDS_PATH)) {
            dm.createPath(BLUR_REGISTERED_SHARDS_PATH);
        }
        if (!dm.exists(BLUR_ONLINE_PATH)) {
            dm.createPath(BLUR_ONLINE_PATH);
        }
        if (!dm.exists(BLUR_ONLINE_SHARDS_PATH)) {
            dm.createPath(BLUR_ONLINE_SHARDS_PATH);
        }
        if (!dm.exists(BLUR_ONLINE_CONTROLLERS_PATH)) {
            dm.createPath(BLUR_ONLINE_CONTROLLERS_PATH);
        }
    }
}
