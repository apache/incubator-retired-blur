package org.apache.blur.demo;

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
import static org.apache.blur.utils.BlurUtil.newColumn;
import static org.apache.blur.utils.BlurUtil.newRecordMutation;
import static org.apache.blur.utils.BlurUtil.newRowMutation;

import java.util.List;
import java.util.Random;

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.Selector;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.thrift.TException;


public class SampleProgram {

  private static final String BLUR_CLUSTER_NAME = "default";
  public static final String BLUR_CONTROLLER_HOSTNAME = "localhost";
  public static final String BLUR_CONTROLLER_PORT = "40010";

  public static final String HDFS_NAMENODE_HOSTNAME = "localhost";
  public static final String HDFS_NAMENODE_PORT = "9000";

  // Path must include trailing slash character
  public static final String BLUR_TABLES_LOCATION = "/blur/tables/";
  private static final int MAX_SAMPLE_ROWS = 1000;
  private static final int MAX_SEARCHES = 500;

  public static void main(String[] args) {
    try {
      // Connect
      Blur.Iface client = connect();

      // Delete all tables
      deleteAllTables(client);

      // Create a table
      String tableName = "SAMPLE_TABLE_" + System.currentTimeMillis();
      createTable(client, tableName);

      // List all the tables
      listTables(client);

      // Populate the table with data
      populateTable(client, tableName);

      // Run searches
      searchTable(client, tableName);

      // Delete the table
      deleteTable(client, tableName);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private static void deleteAllTables(Iface client) throws BlurException, TException {
    List<String> tableList = listTables(client);
    for (String tableName : tableList) {
      deleteTable(client, tableName);
    }
  }

  private static Blur.Iface connect() {
    String blurConnectionString = BLUR_CONTROLLER_HOSTNAME + ":" + BLUR_CONTROLLER_PORT;
    System.out.println("Connecting to " + blurConnectionString);
    Blur.Iface client = BlurClient.getClient(blurConnectionString);
    System.out.println("Successfully connected to " + blurConnectionString);
    return client;
  }

  private static void createTable(Iface client, String tableName) {
    try {
      AnalyzerDefinition ad = new AnalyzerDefinition();

      TableDescriptor tableDescriptor = new TableDescriptor();
      tableDescriptor.setTableUri("hdfs://" + HDFS_NAMENODE_HOSTNAME + ":" + HDFS_NAMENODE_PORT + BLUR_TABLES_LOCATION + tableName);
      tableDescriptor.setAnalyzerDefinition(ad);
      tableDescriptor.setName(tableName);
      tableDescriptor.setCluster(BLUR_CLUSTER_NAME);

      System.out.println("About to create table " + tableName);
      client.createTable(tableDescriptor);
      System.out.println("Created table " + tableName);
    } catch (BlurException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  private static List<String> listTables(Blur.Iface client) {
    try {
      System.out.println("Listing all tables");
      List<String> tableNames = client.tableList();
      for (String tableName : tableNames) {
        System.out.println("tableName=" + tableName);
      }
      return tableNames;
    } catch (BlurException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static void populateTable(Iface client, String tableName) throws BlurException, TException {
    Random random = new Random();
    for (int count = 1; count <= MAX_SAMPLE_ROWS; count++) {
      RowMutation mutation = newRowMutation(tableName, "rowid_" + count, newRecordMutation("sample", "recordid_1", newColumn("sampleData", "data_" + random.nextInt(50000))));
      System.out.println("About to add rowid_" + count);
      client.mutate(mutation);
      System.out.println("Added rowid_" + count);
    }

  }

  private static void searchTable(Iface client, String tableName) throws BlurException, TException {
    Random random = new Random();
    for (int count = 1; count <= MAX_SEARCHES; count++) {
      String rowid = "rowid_" + random.nextInt(MAX_SAMPLE_ROWS);
      Selector selector = new Selector();
      selector.setRowId(rowid);
      FetchResult fetchRow = client.fetchRow(tableName, selector);
      if (fetchRow != null) {
        FetchRowResult rowResult = fetchRow.getRowResult();
        if (rowResult != null) {
          Row row = rowResult.getRow();
          if (row != null) {
            System.out.println("Found " + rowid);
          }

        }
      } else {
        System.out.println("Could not find " + rowid);
      }
    }

  }

  private static void deleteTable(Iface client, String tableName) throws BlurException, TException {
    client.disableTable(tableName);
    client.removeTable(tableName, true);
    System.out.println("Deleted table " + tableName);
  }

}
