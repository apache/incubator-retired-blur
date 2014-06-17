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
package org.apache.blur.console.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.console.ConsoleTestBase;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

public class ConfigTest extends ConsoleTestBase {
	
	@Before
	public void setup() throws IOException {
		Config.setupConfig();
	}

	@Test
	public void testGetConsolePort() {
		assertEquals(8080, Config.getConsolePort());
	}

	@Test
	public void testGetBlurConfig() {
		BlurConfiguration blurConfig = Config.getBlurConfig();
		assertNotNull(blurConfig);
	}

	@Test
	public void testGetConnectionString() throws IOException {
		assertTrue(StringUtils.contains(Config.getConnectionString(), InetAddress.getLocalHost().getHostName()));
	}
	
	@Test
	public void testGetZookeeper() {
		assertNotNull(Config.getZookeeper());
	}
}
