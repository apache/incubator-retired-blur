package com.nearinfinity.agent.zookeeper.collectors;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ShardCollector extends Collector {
	private int clusterId;
	private String clusterName;

//	private static final Log log = LogFactory.getLog(ShardCollector.class);

	private ShardCollector(InstanceManager manager, int clusterId, String clusterName) throws KeeperException,
			InterruptedException {
		super(manager);
		this.clusterId = clusterId;
		this.clusterName = clusterName;

		updateShards();
	}

	private void updateShards() throws KeeperException, InterruptedException {
		List<String> shards = getShards();
		markOfflineShards(shards);
		updateOnlineShards(shards);
	}

	private List<String> getShards() throws KeeperException, InterruptedException {
		return getZk().getChildren("/blur/clusters/" + clusterName + "/online/shard-nodes", false);
	}

	private void markOfflineShards(List<String> shards) {
		if (shards.isEmpty()) {
			getJdbc().update("update shards set status = 0 where cluster_id = ?", clusterId);
		} else {
			getJdbc().update(
					"update shards set status = 0 where node_name not in ('" + StringUtils.join(shards, "','")
							+ "') and cluster_id=?", clusterId);
		}
	}

	private void updateOnlineShards(List<String> shards) throws KeeperException, InterruptedException {
		for (String shard : shards) {
			String blurVersion = "UNKNOWN";

			byte[] b = getZk().getData("/blur/clusters/" + clusterName + "/online/shard-nodes", false, null);
			if (b != null && b.length > 0) {
				blurVersion = new String(b);
			}

			int updatedCount = getJdbc().update(
					"update shards set status=1, blur_version=? where node_name=? and cluster_id=?", blurVersion,
					shard, clusterId);

			if (updatedCount == 0) {
				getJdbc().update(
						"insert into shards (node_name, status, cluster_id, blur_version) values (?, 1, ?, ?)", shard,
						clusterId, blurVersion);
			}
		}
	}

	public static void collect(InstanceManager manager, int clusterId, String clusterName) throws KeeperException,
			InterruptedException {
		new ShardCollector(manager, clusterId, clusterName);
	}
}
