package org.apache.blur.jdbc;

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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.blur.jdbc.abstractimpl.AbstractBlurConnection;

public class BlurConnection extends AbstractBlurConnection {

  private boolean closed;
  private String connectionString;
  private String catalog;
  private String url;
  private String username;

  public BlurConnection(String url, String username, String connectionString, String catalog) {
    this.connectionString = connectionString;
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
    return new BlurDatabaseMetaData(url, username, connectionString);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new BlurStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new BlurPreparedStatement(this, sql);
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

  public String getConnectionString() {
    return connectionString;
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return TRANSACTION_NONE;
  }

}
