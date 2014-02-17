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

package org.apache.blur.console;

import java.io.File;
import java.io.IOException;

import org.apache.blur.MiniCluster;
import org.apache.blur.console.util.Config;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ConsoleTestBase {
	protected static MiniCluster cluster;
	protected static String TABLE_PATH = new File("./test-data/test-tables").getAbsolutePath();
	
	@BeforeClass
	public static void startup() {
		cluster = new MiniCluster();
		cluster.startBlurCluster(new File("./test-data").getAbsolutePath(), 1, 1);
	}
	
	@AfterClass
	public static void shutdown() throws IOException {
		cluster.shutdownBlurCluster();
		File file = new File("./test-data");
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
	}
	
	protected void setupConfigIfNeeded() throws IOException {
		if (Config.getBlurConfig() == null) {
			Config.setupConfig();
		}
	}
}
