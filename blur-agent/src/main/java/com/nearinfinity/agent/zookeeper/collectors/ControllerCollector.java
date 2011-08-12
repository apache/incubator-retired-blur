package com.nearinfinity.agent.zookeeper.collectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.zookeeper.InstanceManager;
import com.nearinfinity.agent.zookeeper.ZookeeperInstance;

public class ControllerCollector {
	private ZooKeeper zk;
	private int instanceId;
	private JdbcTemplate jdbc;
	private Properties props;
	private ZookeeperInstance zki;
	
	private ControllerCollector(InstanceManager manager, JdbcTemplate jdbc, Properties props, ZookeeperInstance zki) {
		this.zk = manager.getInstance();
		this.instanceId = manager.getInstanceId();
		this.jdbc = jdbc;
		this.props = props;
		this.zki = zki;
		
		updateControllers();
	}
	
	private void updateControllers() {
		List<String> onlineControllers = getControllers();
		markOfflineControllers(onlineControllers);
		updateOnlineControllers(onlineControllers);
	}
	
	private void markOfflineControllers(List<String> controllers) {
		if (controllers.isEmpty()) {
			jdbc.update("update controllers set status = 0");
		} else {
			jdbc.update("update controllers set status = 0 where node_name not in ('" + StringUtils.join(controllers, "','") + "')");
		}
	}
	
	private void updateOnlineControllers(List<String> controllers) {
		for (String controller : controllers) {
			// TODO: Get information on each controller (i.e. URI, Enabled, etc.) once we have controllers
			
			List<Map<String, Object>> instances = jdbc.queryForList("select id from controllers where node_name = ?", new Object[]{controller});
			if (instances.isEmpty()) {
				jdbc.update("insert into controllers (node_name, node_location, status, zookeeper_id, blur_version) values (?, ?, ?, ?, ?)", new Object[]{controller, "placeholder", 2, instanceId, "1.0"});
			} else {
				jdbc.update("update controllers set status=2, blur_version=? where node_name=?", new Object[]{"1.0", controller});
			}
		}
	}
	
	private List<String> getControllers() {
		try {
			return zk.getChildren("/blur/online-controller-nodes", false);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	public static void collect(InstanceManager manager, JdbcTemplate jdbc, Properties props, ZookeeperInstance zki) {
		new ControllerCollector(manager, jdbc, props, zki);
	}
}
