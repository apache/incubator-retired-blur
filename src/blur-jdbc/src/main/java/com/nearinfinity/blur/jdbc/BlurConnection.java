package com.nearinfinity.blur.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurConnection;

public class BlurConnection extends AbstractBlurConnection {

  private boolean closed;
  private int port;
  private String host;
  private String catalog;
  private String url;
  private String username;

  public BlurConnection(String url, String username, String host, int port, String catalog) {
    this.host = host;
    this.port = port;
    this.catalog = catalog;
    this.url = url;
    this.username = username;
  }

  @Override
  public void close() throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    return catalog;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new BlurDatabaseMetaData(url, username, host, port);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new BlurStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new BlurPreparedStatement(this,sql);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return new BlurWarnings();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setAutoCommit(boolean arg0) throws SQLException {

  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public void rollback() throws SQLException {

  }

  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return TRANSACTION_NONE;
  }

}
