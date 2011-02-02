package com.nearinfinity.blur.manager.indexserver;

import java.util.List;

public abstract class ClusterStatus {

    public abstract List<String> getOnlineShardServers();

    public abstract List<String> controllerServerList();

    public abstract List<String> shardServerList();

}
