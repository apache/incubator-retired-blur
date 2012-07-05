package com.nearinfinity.blur.jdbc;

import java.sql.ResultSetMetaData;
import java.util.List;

import com.nearinfinity.blur.jdbc.parser.Parser;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.Connection;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurResultSetTest {

  public static void main(String[] args) throws Exception {
//    String sql = "select * from test-table.fam0 where fam0.col0 = 'abroad'";
    String sql = "select * from test-table.fam0";
    List<Connection> connections = BlurClientManager.getConnections("10.192.56.10:40010");
//    BlurResultSetRows resultSet = new BlurResultSetRows(sql,connections);
    
    Iface client = BlurClient.getClient(connections);
    Parser parser = new Parser();
    parser.parse(sql);
    
    BlurResultSetRecords resultSet = new BlurResultSetRecords(client,parser);
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
