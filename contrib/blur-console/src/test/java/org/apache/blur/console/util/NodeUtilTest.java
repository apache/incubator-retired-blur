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
		
		List<Map<String, int[][]>> chartData = (List<Map<String, int[][]>>) nodeStatus.get("chart");
		
		assertEquals(1, chartData.get(0).get("data")[0][1]);
		assertEquals(0, chartData.get(1).get("data")[0][1]);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetZookeeperStatus() throws BlurException, IOException, TException {
		Map<String, Object> nodeStatus = NodeUtil.getZookeeperStatus();
		
		List<Map<String, int[][]>> chartData = (List<Map<String, int[][]>>) nodeStatus.get("chart");
		
		assertEquals(1, chartData.get(0).get("data")[0][1]);
		assertEquals(0, chartData.get(1).get("data")[0][1]);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testGetClusterStatus() throws BlurException, IOException, TException {
		List<Map<String, Object>> nodeStatus = NodeUtil.getClusterStatus();
		
		List<Map<String, int[][]>> chartData = (List<Map<String, int[][]>>) nodeStatus.get(0).get("chart");
		
		assertEquals(0, chartData.get(0).get("data")[0][1]);
		assertEquals(2, chartData.get(1).get("data")[0][1]);
	}
}
