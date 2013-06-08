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

import org.apache.blur.agent.connections.zookeeper.interfaces.ControllerDatabaseInterface;
import org.apache.blur.agent.notifications.Notifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


public class ControllerCollector implements Runnable {
	private static final Log log = LogFactory.getLog(ControllerCollector.class);

	private final int zookeeperId;
	private final ZooKeeper zookeeper;
	private final ControllerDatabaseInterface database;

	public ControllerCollector(int zookeeperId, ZooKeeper zookeeper, ControllerDatabaseInterface database) {
		this.zookeeperId = zookeeperId;
		this.zookeeper = zookeeper;
		this.database = database;
	}

	@Override
	public void run() {
		try {
			List<String> onlineControllers = this.zookeeper.getChildren("/blur/online-controller-nodes", false);
			int recentlyOffline = this.database.markOfflineControllers(onlineControllers, this.zookeeperId);
			if (recentlyOffline > 0) {
				Notifier.getNotifier().notifyControllerOffline(this.database.getRecentOfflineControllerNames(recentlyOffline));
			}
			updateOnlineControllers(onlineControllers);
		} catch (KeeperException e) {
			log.error("Error talking to zookeeper in ControllerCollector.", e);
		} catch (InterruptedException e) {
			log.error("Zookeeper session expired in ControllerCollector.", e);
		}

	}

	private void updateOnlineControllers(List<String> controllers) throws KeeperException, InterruptedException {
		for (String controller : controllers) {
			String blurVersion = "UNKNOWN";

			byte[] b = this.zookeeper.getData("/blur/online-controller-nodes/" + controller, false, null);
			if (b != null && b.length > 0) {
				blurVersion = new String(b);
			}

			this.database.updateOnlineController(controller, zookeeperId, blurVersion);
		}
	}
}
