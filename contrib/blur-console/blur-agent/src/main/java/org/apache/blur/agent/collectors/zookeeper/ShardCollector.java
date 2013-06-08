package org.apache.blur.agent.collectors.zookeeper;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.List;

import org.apache.blur.agent.connections.zookeeper.interfaces.ShardsDatabaseInterface;
import org.apache.blur.agent.notifications.Notifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


public class ShardCollector implements Runnable {
	private static final Log log = LogFactory.getLog(ShardCollector.class);

	private final int clusterId;
	private final String clusterName;
	private final ZooKeeper zookeeper;
	private final ShardsDatabaseInterface database;

	public ShardCollector(int clusterId, String clusterName, ZooKeeper zookeeper, ShardsDatabaseInterface database) {
		this.clusterId = clusterId;
		this.clusterName = clusterName;
		this.zookeeper = zookeeper;
		this.database = database;
	}

	@Override
	public void run() {
		try {
			List<String> shards = this.zookeeper.getChildren("/blur/clusters/" + clusterName + "/online/shard-nodes", false);
			int recentlyOffline = this.database.markOfflineShards(shards, this.clusterId);
			if (recentlyOffline > 0) {
				Notifier.getNotifier().notifyShardOffline(this.database.getRecentOfflineShardNames(recentlyOffline));
			}
			updateOnlineShards(shards);
		} catch (KeeperException e) {
			log.error("Error talking to zookeeper in ShardCollector.", e);
		} catch (InterruptedException e) {
			log.error("Zookeeper session expired in ShardCollector.", e);
		}
	}

	private void updateOnlineShards(List<String> shards) throws KeeperException, InterruptedException {
		for (String shard : shards) {
			String blurVersion = "UNKNOWN";

			byte[] b = this.zookeeper.getData("/blur/clusters/" + clusterName + "/online/shard-nodes/" + shard, false, null);
			if (b != null && b.length > 0) {
				blurVersion = new String(b);
			}

			this.database.updateOnlineShard(shard, this.clusterId, blurVersion);
		}
	}
}
