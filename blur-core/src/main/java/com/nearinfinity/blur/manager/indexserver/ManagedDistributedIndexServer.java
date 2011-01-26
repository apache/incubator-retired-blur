package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nearinfinity.blur.manager.indexserver.DistributedManager.Value;

public abstract class ManagedDistributedIndexServer extends DistributedIndexServer implements ZookeeperPathContants {
    
    public enum NODE_TYPE {
        SHARD,
        CONTROLLER
    }
    
    private static final Log LOG = LogFactory.getLog(ManagedDistributedIndexServer.class);
    
    private DistributedManager dm;

    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private List<String> onlineShards = new ArrayList<String>();
    private Timer daemon;
    private long zkPollDelay = TimeUnit.MINUTES.toMillis(1);
    private NODE_TYPE type;
    
    @Override
    public ManagedDistributedIndexServer init() {
        super.init();
        setupZookeeper();
        lockNodeState();
        try {
            registerMyself();
            setSafeModeStartupIfNeeded();
        } finally {
            unlockNodeState();
        }
        waitIfInSafeMode();
        startPollingDaemon();
        pollForState();
        return this;
    }

    private void lockNodeState() {
        dm.createPath(BLUR_SAFEMODE);
        dm.lock(BLUR_SAFEMODE_LOCK);
    }

    private void unlockNodeState() {
        dm.unlock(BLUR_SAFEMODE_LOCK);
    }

    private void setSafeModeStartupIfNeeded() {
        List<String> list = dm.list(BLUR_ONLINE_SHARDS_PATH);
        if (list.size() == 0) {
            throw new RuntimeException("This node [" + getNodeName() + "] should have been registered.");
        }
        if (list.size() == 1) {
            if (!list.contains(getNodeName())) {
                throw new RuntimeException("This node [" + getNodeName() + "] should have been" +
                		" registered, and should have been online.");
            }
            LOG.info("Setuping safe mode, first node online.");
            dm.createPath(BLUR_SAFEMODE);
            dm.saveData(getSafeModeEndTime(),BLUR_SAFEMODE);
        }
    }

    private void waitIfInSafeMode() {
        Value value = new Value();
        dm.fetchData(value, BLUR_SAFEMODE);
        long waitUntil = getLong(value.data, 0);
        try {
            long waitTime =  waitUntil - System.currentTimeMillis();
            if (waitTime > 0L) {
                LOG.info("Safe Mode On - Will resume in [" + TimeUnit.MILLISECONDS.toSeconds(waitTime) +
                		"] seconds at [" + new Date(waitUntil) + "]");
                Thread.sleep(waitTime);
                LOG.info("Safe Mode Off");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getSafeModeEndTime() {
        long currentTimeMillis = System.currentTimeMillis();
        currentTimeMillis += TimeUnit.SECONDS.toMillis(20);
        byte[] buffer = new byte[8];
        putLong(buffer, 0, currentTimeMillis);
        return buffer;
    }
    
    private void registerMyself() {
        String path;
        if (type == NODE_TYPE.SHARD) {
            if (!dm.exists(BLUR_REGISTERED_SHARDS_PATH,getNodeName())) {
                dm.createPath(BLUR_REGISTERED_SHARDS_PATH,getNodeName());
            }
            path = BLUR_ONLINE_SHARDS_PATH;
        } else {
            path = BLUR_ONLINE_CONTROLLERS_PATH;
        }
        while (dm.exists(path,getNodeName())) {
            LOG.info("Waiting to register myself [" + getNodeName() + "].");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        dm.createEphemeralPath(path,getNodeName());
        dm.removeEphemeralPathOnShutdown(path,getNodeName());
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
    
    public DistributedManager getDistributedManager() {
        return dm;
    }

    public ManagedDistributedIndexServer setZk(DistributedManager distributedManager) {
        this.dm = distributedManager;
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

    public NODE_TYPE getType() {
        return type;
    }

    public void setType(NODE_TYPE type) {
        this.type = type;
    }
    
    private static long getLong(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL) << 0) + ((b[off + 6] & 0xFFL) << 8) + ((b[off + 5] & 0xFFL) << 16)
                + ((b[off + 4] & 0xFFL) << 24) + ((b[off + 3] & 0xFFL) << 32) + ((b[off + 2] & 0xFFL) << 40)
                + ((b[off + 1] & 0xFFL) << 48) + (((long) b[off + 0]) << 56);
    }

    private static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val >>> 0);
        b[off + 6] = (byte) (val >>> 8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off + 0] = (byte) (val >>> 56);
    }
}
