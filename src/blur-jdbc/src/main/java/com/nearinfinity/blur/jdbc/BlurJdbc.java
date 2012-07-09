package com.nearinfinity.blur.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class BlurJdbc implements Driver {

  static {
    try {
      java.sql.DriverManager.registerDriver(new BlurJdbc());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    // jdbc:blur:host:port
    String[] split = url.split(":");
    if (split.length != 4) {
      return false;
    }
    if ("jdbc".equals(split[0])) {
      if ("blur".equals(split[1])) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    // jdbc:blur:host:port
    String username = "";
    String[] split = url.split(":");
    if (split.length != 4) {
      throw new SQLException("Invalid url [" + url + "]");
    }
    if ("jdbc".equals(split[0])) {
      if ("blur".equals(split[1])) {
        String host = split[2];
        int port = Integer.parseInt(split[3]);
        return new BlurConnection(url, username, host, port, host);
      }
    }
    throw new SQLException("Invalid url [" + url + "]");
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

}
