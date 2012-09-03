package com.nearinfinity.blur.jdbc;

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
import java.sql.ResultSetMetaData;
import java.util.List;

import com.nearinfinity.blur.jdbc.parser.Parser;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.Connection;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurResultSetTest {

  public static void main(String[] args) throws Exception {
    // String sql = "select * from test-table.fam0 where fam0.col0 = 'abroad'";
    String sql = "select * from test-table.fam0";
    List<Connection> connections = BlurClientManager.getConnections("10.192.56.10:40010");
    // BlurResultSetRows resultSet = new BlurResultSetRows(sql,connections);

    Iface client = BlurClient.getClient(connections);
    Parser parser = new Parser();
    parser.parse(sql);

    BlurResultSetRecords resultSet = new BlurResultSetRecords(client, parser);
    int c = 0;
    while (resultSet.next()) {
      System.out.println(c + " ------------------------");
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String value = resultSet.getString(i);
        String name = metaData.getColumnName(i);
        System.out.println("\t" + name + ":[" + value + "]");
      }
      c++;
    }
  }

}
