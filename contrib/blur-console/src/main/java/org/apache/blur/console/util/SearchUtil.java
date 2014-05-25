package org.apache.blur.console.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.Selector;

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

public class SearchUtil {
	
	/**
	 * Record only results:
	 * 
	 * {
	 * 	famName: [
	 * 		{
	 * 			colName: value
	 * 		}
	 * 	]
	 * }
	 * 
	 * Row results:
	 * {
	 * 	famName: {
	 * 		rowid: [
	 * 			{
	 * 				colName: value
	 * 			}
	 * 		]
	 * 	}
	 * }
	 */
	
	@SuppressWarnings("unchecked")
	public static Map<String, Object> search(Map<String, String[]> params) throws IOException, BlurException, TException {
		Iface client = BlurClient.getClient(Config.getConnectionString());
		
		String table = params.get("table")[0];
		String query = params.get("query")[0];
		String rowQuery = params.get("rowRecordOption")[0];
		String start = params.get("start")[0];
		String fetch = params.get("fetch")[0];
		String[] families = params.get("families[]");
		
		boolean recordsOnly = "recordrecord".equalsIgnoreCase(rowQuery);
		
		BlurQuery blurQuery = new BlurQuery();
		
		Query q = new Query(query, "rowrow".equalsIgnoreCase(rowQuery), ScoreType.SUPER, null, null);
		blurQuery.setQuery(q);
		blurQuery.setStart(Long.parseLong(start));
		blurQuery.setFetch(Integer.parseInt(fetch));
		
		Selector s = new Selector();
		s.setRecordOnly(recordsOnly);
		s.setColumnFamiliesToFetch(new HashSet<String>(Arrays.asList(families)));
		blurQuery.setSelector(s);
		
		BlurResults blurResults = client.query(table, blurQuery);
		
		Map<String, Object> results = new HashMap<String, Object>();
		results.put("total", blurResults.getTotalResults());
		
		Set<String> fams = new HashSet<String>();
		Map<String, Object> rows = new HashMap<String, Object>();
		for (BlurResult result : blurResults.getResults()) {
			FetchResult fetchResult = result.getFetchResult();
			
			if (recordsOnly) {
				// Record Result
				FetchRecordResult recordResult = fetchResult.getRecordResult();
				Record record = recordResult.getRecord();
				
				String family = record.getFamily();
				fams.add(family);
				
				List<Map<String, String>> fam = (List<Map<String, String>>) getFam(family, rows, recordsOnly);
				fam.add(buildRow(record.getColumns(), record.getRecordId()));
			} else {
				// Row Result
				FetchRowResult rowResult = fetchResult.getRowResult();
				Row row = rowResult.getRow();
				for (Record record : row.getRecords()) {
					String family = record.getFamily();
					fams.add(family);
					
					Map<String, List<Map<String, String>>> fam = (Map<String, List<Map<String, String>>>) getFam(family, rows, recordsOnly);
					List<Map<String, String>> rowData = getRow(row.getId(), fam);
					rowData.add(buildRow(record.getColumns(), record.getRecordId()));
				}
			}
		}
		
		results.put("families", fams);
		results.put("results", rows);
		
		return results;
	}
	
	private static Map<String, String> buildRow(List<Column> columns, String recordid) {
		Map<String, String> map = new TreeMap<String, String>();
		map.put("recordid", recordid);
		
		for (Column column : columns) {
			map.put(column.getName(), column.getValue());
		}
		
		return map;
	}
	
	private static Object getFam(String fam, Map<String, Object> results, boolean recordOnly) {
		Object famResults = results.get(fam);
		
		if (famResults == null) {
			if (recordOnly) {
				famResults = new ArrayList<Map<String, String>>();				
			} else {
				famResults = new TreeMap<String, List<Map<String, String>>>();
			}
			results.put(fam, famResults);
		}
		
		return famResults;
	}
	
	private static List<Map<String, String>> getRow(String rowid, Map<String, List<Map<String, String>>> rows) {
		List<Map<String, String>> row = rows.get(rowid);
		
		if (row == null) {
			row = new ArrayList<Map<String, String>>();
			rows.put(rowid, row);
		}
		
		return row;
	}
}
