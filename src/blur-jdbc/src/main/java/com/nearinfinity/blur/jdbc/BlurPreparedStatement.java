package com.nearinfinity.blur.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurPreparedStatement;

public class BlurPreparedStatement extends AbstractBlurPreparedStatement {

  private BlurStatement blurStatement;
  private String sql;

  public BlurPreparedStatement(BlurConnection connection, String sql) {
    this.sql = sql;
    blurStatement = new BlurStatement(connection);
  }

  @Override
  public boolean execute() throws SQLException {
    return blurStatement.execute(sql);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return blurStatement.getResultSet();
  }
  
}