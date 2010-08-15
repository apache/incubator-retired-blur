package com.nearinfinity.blur.utils;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtils {
	
	private static final Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

	public static void mkNodes(String path, ZooKeeper zk) throws IOException {
		String[] split = path.split("/");
		for (int i = 0; i < split.length; i++) {
			StringBuilder builder = new StringBuilder();
			for (int j = 0; j <= i; j++) {
				if (!split[j].isEmpty()) {
					builder.append('/');
					builder.append(split[j]);
				}
			}
			String pathToCheck = builder.toString();
			if (pathToCheck.isEmpty()) {
				continue;
			}
			try {
				if (zk.exists(pathToCheck, false) == null) {
					zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (NodeExistsException e) {
				//do nothing
			} catch (KeeperException e) {
				LOG.error("error",e);
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				LOG.error("error",e);
				throw new RuntimeException(e);
			}
		}
	}
	
	public static String getPath(String... parts) {
		if (parts == null || parts.length == 0) {
			return null;
		}
		StringBuilder builder = new StringBuilder(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			builder.append('/');
			builder.append(parts[i]);
		}
		return builder.toString();
	}
}
