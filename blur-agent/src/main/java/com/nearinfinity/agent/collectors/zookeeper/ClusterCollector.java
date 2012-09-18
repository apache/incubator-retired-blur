package com.nearinfinity.agent.collectors.zookeeper;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.agent.collectors.connections.interfaces.Collector;
import com.nearinfinity.agent.types.InstanceManager;

public class ClusterCollector extends Collector {

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
			boolean safeMode = isClusterInSafeMode(cluster);
		
			int updateCount = getJdbc().update("update clusters set safe_mode=? where name=? and zookeeper_id=?", safeMode, cluster, zkId);
			if(updateCount == 0){
				getJdbc().update("insert into clusters (name, zookeeper_id, safe_mode) values (?, ?, ?)", cluster, zkId, safeMode);
			}
			List<Map<String, Object>> instances = getJdbc().queryForList(
					"select id from clusters where name = ? and zookeeper_id=?", cluster, zkId);
			int clusterId = (Integer) instances.get(0).get("ID");

			ShardCollector.collect(manager, clusterId, cluster);
			TableCollector.collect(manager, clusterId, cluster);
		}
	}
	
	private boolean isClusterInSafeMode(String cluster) throws KeeperException, InterruptedException {
		String blurSafemodePath = "/blur/clusters/" + cluster + "/safemode";
	      Stat stat = getZk().exists(blurSafemodePath, false);
	      if (stat == null) {
	        return false;
	      }
	      byte[] data = getZk().getData(blurSafemodePath, false, stat);
	      if (data == null) {
	        return false;
	      }
	      long timestamp = Long.parseLong(new String(data));
	      long waitTime = timestamp - System.currentTimeMillis();
	      if (waitTime > 0) {
	        return true;
	      }
	      return false;
	}

	private List<String> getClusters() throws KeeperException, InterruptedException {
		return getZk().getChildren("/blur/clusters", false);
	}

	public static void collect(InstanceManager manager, int zkId) throws KeeperException, InterruptedException {
		new ClusterCollector(manager, zkId);
	}
}
