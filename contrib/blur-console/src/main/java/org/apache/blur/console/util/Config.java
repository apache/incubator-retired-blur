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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.commons.lang.StringUtils;

public class Config {
	private static int port = 8080;
	private static BlurConfiguration blurConfig;
	private static ZookeeperClusterStatus zk;
	private static String blurConnection;

	public static int getConsolePort() {
		return port;
	}
	public static BlurConfiguration getBlurConfig() {
		return blurConfig;
	}
	
	public static void setupConfig() throws IOException {
		blurConfig = new BlurConfiguration();
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
}
