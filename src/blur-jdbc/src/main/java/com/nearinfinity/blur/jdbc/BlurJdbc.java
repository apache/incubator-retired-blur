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
