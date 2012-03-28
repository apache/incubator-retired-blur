package com.nearinfinity.agent.zookeeper.collectors;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.nearinfinity.agent.zookeeper.InstanceManager;

public class ControllerCollector extends Collector {

	private static final Log log = LogFactory.getLog(ControllerCollector.class);
	private int zkId;

	private ControllerCollector(InstanceManager manager, int zkId) throws KeeperException, InterruptedException {
		super(manager);
		this.zkId = zkId;
		collect();
	}

	private void collect() throws KeeperException, InterruptedException {
		List<String> onlineControllers = getControllers();
		markOfflineControllers(onlineControllers);
		updateOnlineControllers(onlineControllers);
	}

	private void markOfflineControllers(List<String> controllers) {
		if (controllers.isEmpty()) {
			getJdbc().update("update controllers set status = 0 where zookeeper_id = ?", zkId);
		} else {
			getJdbc().update(
					"update controllers set status = 0 where node_name not in ('"
							+ StringUtils.join(controllers, "','") + "') and zookeeper_id = ?", zkId);
		}
	}

	private void updateOnlineControllers(List<String> controllers) throws KeeperException, InterruptedException {
		for (String controller : controllers) {
			String blurVersion = "UNKNOWN";

			byte[] b = getZk().getData("/blur/online-controller-nodes", false, null);
			if (b != null && b.length > 0) {
				blurVersion = new String(b);
			}

			int updatedCount = getJdbc().update(
					"update controllers set status=1, blur_version=? where node_name=? and zookeeper_id =?",
					blurVersion, controller, zkId);

			if (updatedCount == 0) {
				getJdbc().update(
						"insert into controllers (node_name, status, zookeeper_id, blur_version) values (?, 1, ?, ?)",
						controller, zkId, blurVersion);
			}
		}
	}

	private List<String> getControllers() throws KeeperException, InterruptedException {
		return getZk().getChildren("/blur/online-controller-nodes", false);
	}

	public static void collect(InstanceManager manager, int zkId) throws KeeperException, InterruptedException {
		new ControllerCollector(manager, zkId);
	}
}
