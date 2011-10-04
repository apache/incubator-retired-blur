package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ControllerCollector {
	private ZooKeeper zk;
	private int instanceId;
	private JdbcTemplate jdbc;
	
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
			// TODO: Get information on each controller (i.e. URI, Enabled, etc.) once we have controllers
			String uri = "placeholder";
			int status = 2;
			String blurVersion = "1.0";			
			
			int updatedCount = jdbc.update("update controllers set node_location=?, status=?, blur_version=? where node_name=? and zookeeper_id =?", uri, status, blurVersion, controller, instanceId);

			if (updatedCount == 0) {
				jdbc.update("insert into controllers (node_name, node_location, status, zookeeper_id, blur_version) values (?, ?, ?, ?, ?)", controller, uri, status, instanceId, blurVersion);
			}
		}
	}
	
	private List<String> getControllers() {
		try {
			return zk.getChildren("/blur/online-controller-nodes", true);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	public static void collect(InstanceManager manager) {
		new ControllerCollector(manager);
	}
}
