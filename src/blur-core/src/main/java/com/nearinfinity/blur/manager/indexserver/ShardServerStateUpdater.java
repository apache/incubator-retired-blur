package com.nearinfinity.blur.manager.indexserver;

import java.util.List;

public interface ShardServerStateUpdater {

    DistributedManager getDistributedManager();

    List<String> getShards();

    void setShards(List<String> shards);

    List<String> getOnlineShards();

    void setOnlineShards(List<String> onlineShards);

    List<String> getOfflineShards();

    void setOfflineShards(List<String> offlineShards);

    List<String> getControllers();

    void setControllers(List<String> controllers);

    void register(String path);

    void onShardServerStateChanged();

}
