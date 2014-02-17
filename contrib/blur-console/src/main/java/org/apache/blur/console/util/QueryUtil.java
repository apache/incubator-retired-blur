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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.CpuTime;
import org.apache.blur.thrift.generated.QueryState;

public class QueryUtil {
	
	/**
	 * {
	 * 	clusters: ['prodA', 'prodB'],
	 * 	counts: {
	 * 		'prodA':{
	 * 			timeToTheMinute: count
	 *  	}
	 *  },
	 *  performance: {
	 *  	'prodA':{
	 *  		timeToTheMinute: avgCpuTime
	 *  	}
	 *  },
	 *  slowQueries: count
	 * }
	 */
	public static Map<String, Object> getQueryStatus() throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		Map<String, Object> queryStatus = new HashMap<String, Object>();
		
		List<String> clusters = client.shardClusterList();
		queryStatus.put("clusters", clusters);
		
		Integer slowQueries = 0;
		
		Calendar slowThreshold = Calendar.getInstance();
		slowThreshold.add(Calendar.MINUTE, -1);
		
		Map<String, Map<Long, Long>> queryCounts = new HashMap<String, Map<Long, Long>>();
		Map<String, Map<Long, Double>> queryPerformances = new HashMap<String, Map<Long, Double>>();
		
		for (String cluster : clusters) {
			List<String> tables = client.tableListByCluster(cluster);

			Map<Long, List<Double>> queryPerfs = new TreeMap<Long, List<Double>>();
			Map<Long, Long> clusterQueries = new TreeMap<Long,Long>();
			
			for (String table : tables) {
				if (client.describe(table).isEnabled()) {
					List<String> queryIds = client.queryStatusIdList(table);				
					
					if (queryIds.isEmpty()) {
						Calendar cal = Calendar.getInstance();
						cal.set(Calendar.SECOND, 0);
						cal.set(Calendar.MILLISECOND, 0);
						clusterQueries.put(cal.getTimeInMillis(), 0L);
						queryPerfs.put(cal.getTimeInMillis(), new ArrayList<Double>(Arrays.asList(new Double[]{0.0})));
					} else {
						for (String queryId : queryIds) {
							BlurQueryStatus status = client.queryStatusById(table, queryId);
							
							Calendar cal = Calendar.getInstance();
							cal.setTimeInMillis(status.getQuery().getStartTime());
							cal.set(Calendar.SECOND, 0);
							cal.set(Calendar.MILLISECOND, 0);
							
							if (QueryState.RUNNING.equals(status.getState()) && cal.getTimeInMillis() < slowThreshold.getTimeInMillis()) {
								slowQueries++;
							}
							
							incrementQueryCount(clusterQueries, cal);
							collectQueryPerformance(queryPerfs, cal, status);
						}
					}
				}
			}
			Map<Long, Double> clusterQueryPerfs = new TreeMap<Long,Double>();
			for (Map.Entry<Long, List<Double>> perfEntry : queryPerfs.entrySet()) {
				double sum = 0.0;
				for (Double perf : perfEntry.getValue()) {
					sum += perf;
				}
				clusterQueryPerfs.put(perfEntry.getKey(), (sum/perfEntry.getValue().size()));
			}
			
			queryCounts.put(cluster, clusterQueries);
			queryPerformances.put(cluster, clusterQueryPerfs);
		}
		
		queryStatus.put("counts", queryCounts);
		queryStatus.put("performance", queryPerformances);
		queryStatus.put("slowQueries", slowQueries);
		
		return queryStatus;
	}
	
	private static void incrementQueryCount(Map<Long, Long> queries, Calendar cal) {
		Long previousCount = queries.get(cal.getTimeInMillis());
		if (previousCount == null) {
			previousCount = 0L;
		}
		
		queries.put(cal.getTimeInMillis(), previousCount + 1);
	}
	
	private static void collectQueryPerformance(Map<Long, List<Double>> queries, Calendar cal, BlurQueryStatus status) {
		List<Double> perfs = queries.get(cal.getTimeInMillis());
		if (perfs == null) {
			perfs = new ArrayList<Double>();
			queries.put(cal.getTimeInMillis(), perfs);
		}
		
		Map<String, CpuTime> cpuTimes = status.getCpuTimes();
		for (CpuTime cpu : cpuTimes.values()) {
			perfs.add(cpu.getRealTime()/1000000.0);
		}
	}
}
