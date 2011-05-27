package com.nearinfinity.blur.manager.indexserver;

import java.util.ArrayList;
import java.util.List;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class ShardServerStatePoller {

    private static final Log LOG = LogFactory.getLog(ShardServerStatePoller.class);

    public void pollForStateChanges(ShardServerStateUpdater updater) {
        DistributedManager dm = updater.getDistributedManager();
        List<String> shardNodes = dm.list(ZookeeperPathConstants.getBlurRegisteredShardsPath());
        updater.setOnlineShards(dm.list(ZookeeperPathConstants.getBlurOnlineShardsPath()));
        updater.setControllers(dm.list(ZookeeperPathConstants.getBlurOnlineControllersPath()));
        List<String> offlineShardNodes = new ArrayList<String>(shardNodes);
        offlineShardNodes.removeAll(updater.getOnlineShards());
        boolean stateChange = false;
        List<String> shards = updater.getShards();
        if (!shardNodes.equals(shards)) {
            LOG.info("Shard servers in the cluster changed from [{0}] to [{1}]", shards, shardNodes);
            stateChange = true;
            updater.setShards(shardNodes);
        }
        List<String> offlineShards = updater.getOfflineShards();
        if (!offlineShardNodes.equals(offlineShards)) {
            LOG.info("Offline shard servers changed from [{0}] to [{1}]", offlineShards, offlineShardNodes);
            stateChange = true;
            updater.setOfflineShards(offlineShardNodes);
        }
        if (stateChange) {
            updater.onShardServerStateChanged();
        }
        updater.register(ZookeeperPathConstants.getBlurRegisteredShardsPath());
        updater.register(ZookeeperPathConstants.getBlurOnlineControllersPath());
        updater.register(ZookeeperPathConstants.getBlurOnlineShardsPath());
    }

}
