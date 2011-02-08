package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.utils.BlurConstants;

public class ZookeeperClusterStatus extends ClusterStatus implements BlurConstants, ZookeeperPathContants {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);
    
    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private List<String> onlineShards = new ArrayList<String>();
    private DistributedManager dm;
    private long zkPollDelay = TimeUnit.SECONDS.toMillis(15);
    private Timer daemon;
    
    public void init() {
        startPollingDaemon();
        pollForState();
    }

    @Override
    public List<String> controllerServerList() {
        return controllers;
    }

    @Override
    public List<String> getOnlineShardServers() {
        return onlineShards;
    }

    @Override
    public List<String> shardServerList() {
        return shards;
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
    
    private synchronized void pollForState() {
        List<String> shardNodes = dm.list(BLUR_REGISTERED_SHARDS_PATH);
        onlineShards = dm.list(BLUR_ONLINE_SHARDS_PATH);
        controllers = dm.list(BLUR_ONLINE_CONTROLLERS_PATH);
        List<String> offlineShardNodes = new ArrayList<String>(shardNodes);
        offlineShardNodes.removeAll(onlineShards);
//        boolean stateChange = false;
        if (!shardNodes.equals(shards)) {
            LOG.info("Shard servers in the cluster changed from [{0}] to [{1}]",shards,shardNodes);
//            stateChange = true;
            shards = shardNodes;
        }
        if (!offlineShardNodes.equals(offlineShards)) {
            LOG.info("Offline shard servers changed from [{0}] to [{1}]",offlineShards,offlineShardNodes);
//            stateChange = true;
            offlineShards = offlineShardNodes;
        }
//        if (stateChange) {
//            shardServerStateChange();
//        }
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

    public DistributedManager getDistributedManager() {
        return dm;
    }

    public void setDistributedManager(DistributedManager dm) {
        this.dm = dm;
    }

}
