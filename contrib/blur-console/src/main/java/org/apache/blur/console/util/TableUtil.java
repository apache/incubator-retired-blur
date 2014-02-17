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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.blur.console.model.Column;
import org.apache.blur.console.model.Family;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;

public class TableUtil {
	public static Map<String, Object> getTableStatus() throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		List<String> clusters = client.shardClusterList();

		Map<String, Object> data = new HashMap<String, Object>();
		List<Map<String, Object>> clusterInfo = new ArrayList<Map<String,Object>>();
		
		int clusterCount = 0;
		for (String cluster : clusters) {
			Map<String, Object> clusterObj = new HashMap<String, Object>();
			clusterObj.put("name", cluster);

			List<String> tables = client.tableListByCluster(cluster);
			
			List<String> enabledTables = new ArrayList<String>();
			List<String> disabledTables = new ArrayList<String>();
			int enabledCount = 0;
			int disabledCount = 0;
			
			for (String table : tables) {
				boolean enabled = client.describe(table).isEnabled();
				
				if (enabled) {
					enabledTables.add(table);
					enabledCount++;
				} else {
					disabledTables.add(table);
					disabledCount++;
				}
			}
			
			clusterObj.put("enabled", enabledTables);
			clusterObj.put("disabled", disabledTables);
			
			List<Object> e = new ArrayList<Object>();
			e.add(clusterCount);
			e.add(enabledCount);
			
			List<Object> d = new ArrayList<Object>();
			d.add(clusterCount);
			d.add(disabledCount);

			clusterInfo.add(clusterObj);
			clusterCount++;
		}
		
		data.put("tables", clusterInfo);
		
		return data;
	}
	
	public static Map<String, Object> getTableSummaries() throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		Map<String, List<Map<String, Object>>> tablesByCluster = new HashMap<String, List<Map<String,Object>>>();
		
		List<String> clusters = client.shardClusterList();
		
		for (String cluster : clusters) {
			List<String> tables = client.tableListByCluster(cluster);
			List<Map<String, Object>> tableList = new ArrayList<Map<String,Object>>();
			for (String table : tables) {
				Map<String, Object> tableInfo = new HashMap<String, Object>();
				
				TableDescriptor descriptor = client.describe(table);
				
				tableInfo.put("name", table);
				tableInfo.put("enabled", descriptor.isEnabled());
				
				if (descriptor.isEnabled()) {
					TableStats stats = client.tableStats(table);
					tableInfo.put("rowCount", stats.getRowCount());
					tableInfo.put("recordCount", stats.getRecordCount());
				}
				
				tableList.add(tableInfo);
			}
			tablesByCluster.put(cluster, tableList);
		}
		
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("clusters", clusters);
		data.put("tables", tablesByCluster);
		
		return data;
	}
	
	public static Object getSchema(String table) throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		Schema schema = client.schema(table);
		
		List<Family> schemaList = new ArrayList<Family>();
		for (Map.Entry<String, Map<String, ColumnDefinition>> famEntry : schema.getFamilies().entrySet()) {
			Family family = new Family(famEntry.getKey());
			List<Column> columns = new ArrayList<Column>();
			family.setColumns(columns);
			for(Map.Entry<String, ColumnDefinition> colEntry : famEntry.getValue().entrySet()) {
				ColumnDefinition def = colEntry.getValue();
				
				Column column = new Column(colEntry.getKey());
				column.setFullTextIndexed(def.isFieldLessIndexed());
				column.setSubColumn(def.getSubColumnName());
				column.setType(def.getFieldType());
				column.setProps(def.getProperties());
				columns.add(column);
			}
			schemaList.add(family);
		}
		
		return schemaList;
	}
	
	public static List<String> getTerms(String table, String family, String column, String startWith) throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		return client.terms(table, family, column, startWith, (short)10);
	}
	
	public static void disableTable(String table) throws BlurException, TException, IOException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		client.disableTable(table);
	}
	public static void enableTable(String table) throws BlurException, TException, IOException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		client.enableTable(table);
	}
	public static void deleteTable(String table, boolean includeFiles) throws BlurException, TException, IOException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		client.removeTable(table, includeFiles);
	}
}
