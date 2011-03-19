/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.DistributedManager.Value;
import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.*;

public abstract class ManagedDistributedIndexServer extends DistributedIndexServer implements ShardServerStateUpdater {
    
    public enum NODE_TYPE {
        SHARD,
        CONTROLLER
    }
    
    private static final Log LOG = LogFactory.getLog(ManagedDistributedIndexServer.class);
    
    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private List<String> onlineShards = new ArrayList<String>();
    private Timer daemon;
    private long zkPollDelay = TimeUnit.MINUTES.toMillis(1);
    private NODE_TYPE type;
    private ShardServerStatePoller shardServerStatePoller = new ShardServerStatePoller();
    
    @Override
    public void init() {
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
            removeShutdownFlag();
        }
    }

    private void removeShutdownFlag() {
        dm.removePath(BLUR_SAFEMODE_SHUTDOWN);
    }

    private void waitIfInSafeMode() {
        Value value = new Value();
        dm.fetchData(value, BLUR_SAFEMODE);
        long waitUntil = getLong(value.data, 0);
        try {
            long waitTime =  waitUntil - System.currentTimeMillis();
            if (waitTime > 0L) {
                LOG.info("Safe Mode On - Will resume in [{0}] seconds at [{1}]",TimeUnit.MILLISECONDS.toSeconds(waitTime),new Date(waitUntil));
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
            LOG.info("Waiting to register myself [{0}].",getNodeName());
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        dm.createEphemeralPath(path,getNodeName());
        dm.removeEphemeralPathOnShutdown(path,getNodeName());
        LOG.info("Registered [{0}].",getNodeName());
    }

    private synchronized void pollForState() {
        shardServerStatePoller.pollForStateChanges(this);
    }

    @Override
    public void register(String path) {
        dm.registerCallableOnChange(new Runnable() {
            @Override
            public void run() {
                pollForState();
            }
        },path);
    }

    @Override
    public DistributedManager getDistributedManager() {
        return this.dm;
    }

    @Override
    public List<String> getShards() {
        return this.shards;
    }

    @Override
    public void setShards(List<String> shards) {
        this.shards = shards;
    }

    @Override
    public List<String> getOnlineShards() {
        return this.onlineShards;
    }

    @Override
    public void setOnlineShards(List<String> onlineShards) {
        this.onlineShards = onlineShards;
    }

    @Override
    public List<String> getOfflineShards() {
        return this.offlineShards;
    }

    @Override
    public void setOfflineShards(List<String> offlineShards) {
        this.offlineShards = offlineShards;
    }

    @Override
    public List<String> getControllers() {
        return this.controllers;
    }

    @Override
    public void setControllers(List<String> controllers) {
        this.controllers = controllers;
    }

    @Override
    public void onShardServerStateChanged() {
        shardServerStateChange();
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
    
    public long getZkPollDelay() {
        return zkPollDelay;
    }

    public void setZkPollDelay(long zkPollDelay) {
        this.zkPollDelay = zkPollDelay;
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
