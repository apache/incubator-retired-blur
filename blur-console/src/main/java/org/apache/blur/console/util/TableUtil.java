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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.TableStats;

import java.io.IOException;
import java.util.*;

public class TableUtil {

  @SuppressWarnings("rawtypes")
  public static Map<String, List> getTableSummaries() throws TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    List<Map<String, Object>> summaries = new ArrayList<Map<String, Object>>();

    List<String> clusters = client.shardClusterList();

    for (String cluster : clusters) {
      List<String> tables = client.tableListByCluster(cluster);
      for (String table : tables) {
        Map<String, Object> tableInfo = new HashMap<String, Object>();
        tableInfo.put("cluster", cluster);
        tableInfo.put("name", table);
        try {
          TableDescriptor descriptor = client.describe(table);
          tableInfo.put("enabled", descriptor.isEnabled());
          tableInfo.put("readonly", descriptor.isReadOnly());
  
          if (descriptor.isEnabled()) {
            TableStats stats = client.tableStats(table);
            tableInfo.put("rows", stats.getRowCount());
            tableInfo.put("records", stats.getRecordCount());
  
            Schema schema = client.schema(table);
            tableInfo.put("families", new ArrayList<String>(schema.getFamilies().keySet()));
          } else {
            tableInfo.put("rows", "?");
            tableInfo.put("records", "?");
            tableInfo.put("families", new ArrayList<String>());
          }
        } catch(Exception e) {
          tableInfo.put("error", e.getMessage());
        }

        summaries.add(tableInfo);
      }
    }

    Map<String, List> data = new HashMap<String, List>();
    data.put("tables", summaries);
    data.put("clusters", clusters);

    return data;
  }

  public static Map<String, Map<String, Map<String, Object>>> getSchema(String table) throws TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    Schema schema = client.schema(table);

    Map<String, Map<String, Map<String, Object>>> schemaInfo = new TreeMap<String, Map<String, Map<String, Object>>>();
    for (Map.Entry<String, Map<String, ColumnDefinition>> famEntry : schema.getFamilies().entrySet()) {
      Map<String, Map<String, Object>> columns = new TreeMap<String, Map<String, Object>>();
      for (Map.Entry<String, ColumnDefinition> colEntry : famEntry.getValue().entrySet()) {
        Map<String, Object> info = new HashMap<String, Object>();
        ColumnDefinition def = colEntry.getValue();
        info.put("fieldLess", def.isFieldLessIndexed());
        info.put("type", def.getFieldType());
        info.put("extra", def.getProperties());
        columns.put(colEntry.getKey(), info);
      }
      schemaInfo.put(famEntry.getKey(), columns);
    }

    return schemaInfo;
  }

  public static List<String> getTerms(String table, String family, String column, String startWith) throws TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    return client.terms(table, family, column, startWith, (short) 10);
  }

  public static void disableTable(String table) throws TException, IOException {
    Config.getCachingBlurClient().disableTable(table);
  }

  public static void enableTable(String table) throws TException, IOException {
    Config.getCachingBlurClient().enableTable(table);
  }

  public static void deleteTable(String table, boolean includeFiles) throws TException, IOException {
    Config.getCachingBlurClient().removeTable(table, includeFiles);
  }

  public static void copyTable(String srcTable, String destTable, String destLocation, String cluster) throws TException {
    TableDescriptor td = Config.getCachingBlurClient().describe(srcTable);

    td.setTableUri(destLocation);
    td.setCluster(cluster);
    td.setName(destTable);

    CachingBlurClient client = Config.getCachingBlurClient();
    client.createTable(td);
    
    Schema schema = client.schema(srcTable);
    
    for(Map<String, ColumnDefinition> column : schema.getFamilies().values()) {
    	for (ColumnDefinition def : column.values()) {
    		client.addColumnDefinition(destTable, def);
    	}
    }
  }
}
