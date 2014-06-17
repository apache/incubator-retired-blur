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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.blur.console.ConsoleTestBase;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.junit.Before;
import org.junit.Test;

public class NodeUtilTest extends ConsoleTestBase {
	@Before
	public void setup() throws BlurException, TException, IOException {
		setupConfigIfNeeded();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetControllerStatus() throws BlurException, IOException, TException {
		Map<String, Object> nodeStatus = NodeUtil.getControllerStatus();
		
		assertEquals(2, ((List<String>) nodeStatus.get("online")).size());
		assertEquals(0, ((List<String>) nodeStatus.get("offline")).size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetZookeeperStatus() throws BlurException, IOException, TException {
		Map<String, Object> nodeStatus = NodeUtil.getZookeeperStatus();
		
		assertEquals(0, ((HashSet<String>) nodeStatus.get("online")).size());
		assertEquals(1, ((HashSet<String>) nodeStatus.get("offline")).size());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetClusterStatus() throws BlurException, IOException, TException {
		List<Map<String, Object>> nodeStatus = NodeUtil.getClusterStatus();
		
		assertEquals(3, ((List<String>) nodeStatus.get(0).get("online")).size());
		assertEquals(0, ((List<String>) nodeStatus.get(0).get("offline")).size());
	}
}
