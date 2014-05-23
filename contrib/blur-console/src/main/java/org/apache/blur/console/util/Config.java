package org.apache.blur.console.util;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.MiniCluster;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.utils.GCWatcher;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class Config {
	private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/mini-cluster"));
	
	private static int port = 8080;
	private static BlurConfiguration blurConfig;
	private static ZookeeperClusterStatus zk;
	private static String blurConnection;
	private static MiniCluster cluster;

	public static int getConsolePort() {
		return port;
	}
	public static BlurConfiguration getBlurConfig() {
		return blurConfig;
	}
	
	public static void setupConfig() throws IOException {
		if (cluster == null) {
			blurConfig = new BlurConfiguration();
		} else {
			blurConfig = new BlurConfiguration(false);
			blurConfig.set("blur.zookeeper.connection", cluster.getZkConnectionString());
		}
		zk = new ZookeeperClusterStatus(blurConfig.get("blur.zookeeper.connection"), blurConfig);
		blurConnection = buildConnectionString();
	}
	
	public static String getConnectionString() throws IOException {
		return blurConnection;
	}
	
	public static ZookeeperClusterStatus getZookeeper() {
		return zk;
	}
	
	private static String buildConnectionString() {
		List<String> allControllers = new ArrayList<String>();
		allControllers = zk.getControllerServerList();
		return StringUtils.join(allControllers, ",");
	}
	
	public static void shutdownMiniCluster() throws IOException {
		if (cluster != null) {
			cluster.shutdownBlurCluster();
			File file = new File(TMPDIR, "blur-cluster-test");
			if (file.exists()) {
				FileUtils.deleteDirectory(file);
			}
		}
	}
	
	
	public static void setupMiniCluster() throws IOException {
		GCWatcher.init(0.60);
	    LocalFileSystem localFS = FileSystem.getLocal(new Configuration());
	    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
	    testDirectory.mkdirs();

	    Path directory = new Path(testDirectory.getPath());
	    FsPermission dirPermissions = localFS.getFileStatus(directory).getPermission();
	    FsAction userAction = dirPermissions.getUserAction();
	    FsAction groupAction = dirPermissions.getGroupAction();
	    FsAction otherAction = dirPermissions.getOtherAction();

	    StringBuilder builder = new StringBuilder();
	    builder.append(userAction.ordinal());
	    builder.append(groupAction.ordinal());
	    builder.append(otherAction.ordinal());
	    String dirPermissionNum = builder.toString();
	    System.setProperty("dfs.datanode.data.dir.perm", dirPermissionNum);
	    testDirectory.delete();
	    cluster = new MiniCluster();
	    cluster.startBlurCluster(new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true);
	}
}
