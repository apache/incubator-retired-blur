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
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class ZookeeperClusterStatus extends ClusterStatus implements ShardServerStateUpdater {
    
    private static final Log LOG = LogFactory.getLog(ZookeeperClusterStatus.class);
    
    private List<String> controllers = new ArrayList<String>();
    private List<String> offlineShards = new ArrayList<String>();
    private List<String> shards = new ArrayList<String>();
    private List<String> onlineShards = new ArrayList<String>();
    private DistributedManager dm;
    private long zkPollDelay = TimeUnit.SECONDS.toMillis(15);
    private Timer daemon;
    private boolean closed;
    private ShardServerStatePoller shardServerStatePoller = new ShardServerStatePoller();

    public void init() {
        startPollingDaemon();
        pollForState();
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            daemon.cancel();
            daemon.purge();
        }
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

    public void setDistributedManager(DistributedManager dm) {
        this.dm = dm;
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
        // no-op
    }

}
