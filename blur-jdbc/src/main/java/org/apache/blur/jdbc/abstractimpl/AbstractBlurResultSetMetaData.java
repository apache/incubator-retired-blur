package org.apache.blur.jdbc.abstractimpl;

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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.blur.jdbc.util.NotImplemented;

/**
 * This ResultSetMetaData implementation is simply to provide the major of the
 * method implementations that only throw not implemented exceptions. That way
 * it's easier to see what has been implemented in the real class.
 */
public abstract class AbstractBlurResultSetMetaData implements ResultSetMetaData {

  private ResultSetMetaData throwExceptionDelegate;

  public AbstractBlurResultSetMetaData() {
    throwExceptionDelegate = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
        new Class[] { ResultSetMetaData.class }, new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            throw new NotImplemented(method.getName());
          }
        });
  }

  public String getCatalogName(int column) throws SQLException {
    return throwExceptionDelegate.getCatalogName(column);
  }

  public String getColumnClassName(int column) throws SQLException {
    return throwExceptionDelegate.getColumnClassName(column);
  }

  public int getColumnCount() throws SQLException {
    return throwExceptionDelegate.getColumnCount();
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    return throwExceptionDelegate.getColumnDisplaySize(column);
  }

  public String getColumnLabel(int column) throws SQLException {
    return throwExceptionDelegate.getColumnLabel(column);
  }

  public String getColumnName(int column) throws SQLException {
    return throwExceptionDelegate.getColumnName(column);
  }

  public int getColumnType(int column) throws SQLException {
    return throwExceptionDelegate.getColumnType(column);
  }

  public String getColumnTypeName(int column) throws SQLException {
    return throwExceptionDelegate.getColumnTypeName(column);
  }

  public int getPrecision(int column) throws SQLException {
    return throwExceptionDelegate.getPrecision(column);
  }

  public int getScale(int column) throws SQLException {
    return throwExceptionDelegate.getScale(column);
  }

  public String getSchemaName(int column) throws SQLException {
    return throwExceptionDelegate.getSchemaName(column);
  }

  public String getTableName(int column) throws SQLException {
    return throwExceptionDelegate.getTableName(column);
  }

  public boolean isAutoIncrement(int column) throws SQLException {
    return throwExceptionDelegate.isAutoIncrement(column);
  }

  public boolean isCaseSensitive(int column) throws SQLException {
    return throwExceptionDelegate.isCaseSensitive(column);
  }

  public boolean isCurrency(int column) throws SQLException {
    return throwExceptionDelegate.isCurrency(column);
  }

  public boolean isDefinitelyWritable(int column) throws SQLException {
    return throwExceptionDelegate.isDefinitelyWritable(column);
  }

  public int isNullable(int column) throws SQLException {
    return throwExceptionDelegate.isNullable(column);
  }

  public boolean isReadOnly(int column) throws SQLException {
    return throwExceptionDelegate.isReadOnly(column);
  }

  public boolean isSearchable(int column) throws SQLException {
    return throwExceptionDelegate.isSearchable(column);
  }

  public boolean isSigned(int column) throws SQLException {
    return throwExceptionDelegate.isSigned(column);
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return throwExceptionDelegate.isWrapperFor(iface);
  }

  public boolean isWritable(int column) throws SQLException {
    return throwExceptionDelegate.isWritable(column);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return throwExceptionDelegate.unwrap(iface);
  }

}