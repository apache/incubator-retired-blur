package com.nearinfinity.agent.zookeeper.collectors;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ClusterCollector extends Collector {

	private static final Log log = LogFactory.getLog(ClusterCollector.class);

	private int zkId;

	private ClusterCollector(InstanceManager manager, int zkId) throws KeeperException, InterruptedException {
		super(manager);
		this.zkId = zkId;
		collect();
	}

	private void collect() throws KeeperException, InterruptedException {
		List<String> onlineClusters = getClusters();
		updateOnlineClusters(onlineClusters);
	}

	private void updateOnlineClusters(List<String> clusters) throws KeeperException, InterruptedException {
		for (String cluster : clusters) {
			List<Map<String, Object>> instances = getJdbc().queryForList(
					"select id from clusters where name = ? and zookeeper_id=?", cluster, zkId);
			int clusterId;
			if (instances.isEmpty()) {
				getJdbc().update("insert into clusters (name, zookeeper_id) values (?, ?)", cluster, zkId);
				clusterId = getJdbc().queryForInt("select id from clusters where name = ? and zookeeper_id=?", cluster,
						zkId);
			} else {
				clusterId = (Integer) instances.get(0).get("ID");
			}

			ShardCollector.collect(manager, clusterId, cluster);
			TableCollector.collect(manager, clusterId, cluster);
		}
	}

	private List<String> getClusters() throws KeeperException, InterruptedException {
		return getZk().getChildren("/blur/clusters", false);
	}

	public static void collect(InstanceManager manager, int zkId) throws KeeperException, InterruptedException {
		new ClusterCollector(manager, zkId);
	}
}
