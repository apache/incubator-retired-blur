package com.nearinfinity.blur.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

import org.apache.thrift.TException;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurStatement;
import com.nearinfinity.blur.jdbc.parser.Parser;
import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;

public class BlurStatement extends AbstractBlurStatement {

  private BlurConnection connection;
  private int maxRows;
  private String sql;

  public BlurStatement(BlurConnection connection) {
    this.connection = connection;
  }

  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  public void setMaxRows(int maxRows) throws SQLException {
    this.maxRows = maxRows;
  }

  public int getUpdateCount() throws SQLException {
    return -1;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    execute(sql);
    return getResultSet();
  }

  public boolean execute(String sql) throws SQLException {
    this.sql = sql;
    return true;
  }

  public ResultSet getResultSet() throws SQLException {
    try {
      Iface client = BlurClient.getClient(connection.getHost() + ":" + connection.getPort());
      Parser parser = new Parser();
      parser.parse(sql);
      if (isSuperQuery(parser, client)) {
        return new BlurResultSetRows(client, parser);
      } else {
        return new BlurResultSetRecords(client, parser);
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException("Unknown Error", e);
    }
  }

  private boolean isSuperQuery(Parser parser, Iface client) throws BlurException, TException, SQLException {
    String tableName = parser.getTableName();
    List<String> tableList = client.tableList();
    if (tableList.contains(tableName)) {
      return true;
    }
    int lastIndexOf = tableName.lastIndexOf('.');
    if (tableList.contains(tableName.substring(0, lastIndexOf))) {
      return false;
    }
    throw new SQLException("Table [" + tableName + "] does not exist.");
  }

  public void addBatch(String s) throws SQLException {

  }

  public void cancel() throws SQLException {

    // @TODO fix this

    // try {
    // BlurClientManager.execute(connection.getHost() + ":" +
    // connection.getPort(), new BlurCommand<Void>() {
    // @Override
    // public Void call(Client client) throws Exception {
    // client.cancelQuery(uuid);
    // return null;
    // }
    // });
    // } catch (Exception e) {
    // throw new SQLException(e);
    // }
  }

  public void clearBatch() throws SQLException {

  }

  public void clearWarnings() throws SQLException {

  }

  public void close() throws SQLException {

  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public SQLWarning getWarnings() throws SQLException {
    return new BlurWarnings();
  }

}