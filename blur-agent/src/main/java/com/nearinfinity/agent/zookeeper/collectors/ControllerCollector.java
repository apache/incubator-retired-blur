package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ControllerCollector {
	private ZooKeeper zk;
	private int instanceId;
	private JdbcTemplate jdbc;
	
	private static final Log log = LogFactory.getLog(ControllerCollector.class);
	
	private ControllerCollector(InstanceManager manager) {
		this.zk = manager.getInstance();
		this.instanceId = manager.getInstanceId();
		this.jdbc = manager.getJdbc();

		updateControllers();
	}
	
	private void updateControllers() {
		List<String> onlineControllers = getControllers();
		markOfflineControllers(onlineControllers);
		updateOnlineControllers(onlineControllers);
	}
	
	private void markOfflineControllers(List<String> controllers) {
		if (controllers.isEmpty()) {
			jdbc.update("update controllers set status = 0 where zookeeper_id = ?", instanceId);
		} else {
			jdbc.update("update controllers set status = 0 where node_name not in ('" + StringUtils.join(controllers, "','") + "') and zookeeper_id = ?", instanceId);
		}
	}
	
	private void updateOnlineControllers(List<String> controllers) {
		for (String controller : controllers) {
			String blurVersion = "UNKNOWN";
			
			try {
				byte[] b = zk.getData("/blur/online-controller-nodes", true, null);
				if (b != null && b.length > 0) {
					blurVersion = new String(b);
				}
			} catch (Exception e) {
				log.warn("Unable to figure out shard blur version", e);
			}		
			
			int updatedCount = jdbc.update("update controllers set status=1, blur_version=? where node_name=? and zookeeper_id =?", blurVersion, controller, instanceId);

			if (updatedCount == 0) {
				jdbc.update("insert into controllers (node_name, status, zookeeper_id, blur_version) values (?, 1, ?, ?)", controller, instanceId, blurVersion);
			}
		}
	}
	
	private List<String> getControllers() {
		try {
			return zk.getChildren("/blur/online-controller-nodes", true);
		} catch (KeeperException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		}
		return new ArrayList<String>();
	}

	public static void collect(InstanceManager manager) {
		new ControllerCollector(manager);
	}
}
