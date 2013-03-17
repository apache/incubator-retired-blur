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
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.blur.jdbc.util.NotImplemented;

public abstract class AbstractBlurResultSet implements ResultSet {

  private ResultSet throwExceptionDelegate;

  public AbstractBlurResultSet() {
    throwExceptionDelegate = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
        new Class[] { ResultSet.class }, new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            throw new NotImplemented(method.getName());
          }
        });
  }

  public boolean absolute(int row) throws SQLException {
    return throwExceptionDelegate.absolute(row);
  }

  public void afterLast() throws SQLException {
    throwExceptionDelegate.afterLast();
  }

  public void beforeFirst() throws SQLException {
    throwExceptionDelegate.beforeFirst();
  }

  public void cancelRowUpdates() throws SQLException {
    throwExceptionDelegate.cancelRowUpdates();
  }

  public void clearWarnings() throws SQLException {
    throwExceptionDelegate.clearWarnings();
  }

  public void close() throws SQLException {
    throwExceptionDelegate.close();
  }

  public void deleteRow() throws SQLException {
    throwExceptionDelegate.deleteRow();
  }

  public int findColumn(String columnLabel) throws SQLException {
    return throwExceptionDelegate.findColumn(columnLabel);
  }

  public boolean first() throws SQLException {
    return throwExceptionDelegate.first();
  }

  public Array getArray(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getArray(columnIndex);
  }

  public Array getArray(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getArray(columnLabel);
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getAsciiStream(columnIndex);
  }

  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getAsciiStream(columnLabel);
  }

  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return throwExceptionDelegate.getBigDecimal(columnIndex, scale);
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getBigDecimal(columnIndex);
  }

  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return throwExceptionDelegate.getBigDecimal(columnLabel, scale);
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getBigDecimal(columnLabel);
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getBinaryStream(columnIndex);
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getBinaryStream(columnLabel);
  }

  public Blob getBlob(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getBlob(columnIndex);
  }

  public Blob getBlob(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getBlob(columnLabel);
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getBoolean(columnIndex);
  }

  public boolean getBoolean(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getBoolean(columnLabel);
  }

  public byte getByte(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getByte(columnIndex);
  }

  public byte getByte(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getByte(columnLabel);
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getBytes(columnIndex);
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getBytes(columnLabel);
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getCharacterStream(columnIndex);
  }

  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getCharacterStream(columnLabel);
  }

  public Clob getClob(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getClob(columnIndex);
  }

  public Clob getClob(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getClob(columnLabel);
  }

  public int getConcurrency() throws SQLException {
    return throwExceptionDelegate.getConcurrency();
  }

  public String getCursorName() throws SQLException {
    return throwExceptionDelegate.getCursorName();
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getDate(columnIndex, cal);
  }

  public Date getDate(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getDate(columnIndex);
  }

  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getDate(columnLabel, cal);
  }

  public Date getDate(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getDate(columnLabel);
  }

  public double getDouble(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getDouble(columnIndex);
  }

  public double getDouble(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getDouble(columnLabel);
  }

  public int getFetchDirection() throws SQLException {
    return throwExceptionDelegate.getFetchDirection();
  }

  public int getFetchSize() throws SQLException {
    return throwExceptionDelegate.getFetchSize();
  }

  public float getFloat(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getFloat(columnIndex);
  }

  public float getFloat(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getFloat(columnLabel);
  }

  public int getHoldability() throws SQLException {
    return throwExceptionDelegate.getHoldability();
  }

  public int getInt(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getInt(columnIndex);
  }

  public int getInt(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getInt(columnLabel);
  }

  public long getLong(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getLong(columnIndex);
  }

  public long getLong(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getLong(columnLabel);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return throwExceptionDelegate.getMetaData();
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getNCharacterStream(columnIndex);
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getNCharacterStream(columnLabel);
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getNClob(columnIndex);
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getNClob(columnLabel);
  }

  public String getNString(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getNString(columnIndex);
  }

  public String getNString(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getNString(columnLabel);
  }

  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return throwExceptionDelegate.getObject(columnIndex, map);
  }

  public Object getObject(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getObject(columnIndex);
  }

  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return throwExceptionDelegate.getObject(columnLabel, map);
  }

  public Object getObject(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getObject(columnLabel);
  }

  public Ref getRef(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getRef(columnIndex);
  }

  public Ref getRef(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getRef(columnLabel);
  }

  public int getRow() throws SQLException {
    return throwExceptionDelegate.getRow();
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getRowId(columnIndex);
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getRowId(columnLabel);
  }

  public short getShort(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getShort(columnIndex);
  }

  public short getShort(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getShort(columnLabel);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getSQLXML(columnIndex);
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getSQLXML(columnLabel);
  }

  public Statement getStatement() throws SQLException {
    return throwExceptionDelegate.getStatement();
  }

  public String getString(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getString(columnIndex);
  }

  public String getString(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getString(columnLabel);
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getTime(columnIndex, cal);
  }

  public Time getTime(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getTime(columnIndex);
  }

  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getTime(columnLabel, cal);
  }

  public Time getTime(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getTime(columnLabel);
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getTimestamp(columnIndex, cal);
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getTimestamp(columnIndex);
  }

  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return throwExceptionDelegate.getTimestamp(columnLabel, cal);
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getTimestamp(columnLabel);
  }

  public int getType() throws SQLException {
    return throwExceptionDelegate.getType();
  }

  @SuppressWarnings("deprecation")
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getUnicodeStream(columnIndex);
  }

  @SuppressWarnings("deprecation")
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getUnicodeStream(columnLabel);
  }

  public URL getURL(int columnIndex) throws SQLException {
    return throwExceptionDelegate.getURL(columnIndex);
  }

  public URL getURL(String columnLabel) throws SQLException {
    return throwExceptionDelegate.getURL(columnLabel);
  }

  public SQLWarning getWarnings() throws SQLException {
    return throwExceptionDelegate.getWarnings();
  }

  public void insertRow() throws SQLException {
    throwExceptionDelegate.insertRow();
  }

  public boolean isAfterLast() throws SQLException {
    return throwExceptionDelegate.isAfterLast();
  }

  public boolean isBeforeFirst() throws SQLException {
    return throwExceptionDelegate.isBeforeFirst();
  }

  public boolean isClosed() throws SQLException {
    return throwExceptionDelegate.isClosed();
  }

  public boolean isFirst() throws SQLException {
    return throwExceptionDelegate.isFirst();
  }

  public boolean isLast() throws SQLException {
    return throwExceptionDelegate.isLast();
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return throwExceptionDelegate.isWrapperFor(iface);
  }

  public boolean last() throws SQLException {
    return throwExceptionDelegate.last();
  }

  public void moveToCurrentRow() throws SQLException {
    throwExceptionDelegate.moveToCurrentRow();
  }

  public void moveToInsertRow() throws SQLException {
    throwExceptionDelegate.moveToInsertRow();
  }

  public boolean next() throws SQLException {
    return throwExceptionDelegate.next();
  }

  public boolean previous() throws SQLException {
    return throwExceptionDelegate.previous();
  }

  public void refreshRow() throws SQLException {
    throwExceptionDelegate.refreshRow();
  }

  public boolean relative(int rows) throws SQLException {
    return throwExceptionDelegate.relative(rows);
  }

  public boolean rowDeleted() throws SQLException {
    return throwExceptionDelegate.rowDeleted();
  }

  public boolean rowInserted() throws SQLException {
    return throwExceptionDelegate.rowInserted();
  }

  public boolean rowUpdated() throws SQLException {
    return throwExceptionDelegate.rowUpdated();
  }

  public void setFetchDirection(int direction) throws SQLException {
    throwExceptionDelegate.setFetchDirection(direction);
  }

  public void setFetchSize(int rows) throws SQLException {
    throwExceptionDelegate.setFetchSize(rows);
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    return throwExceptionDelegate.unwrap(iface);
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throwExceptionDelegate.updateArray(columnIndex, x);
  }

  public void updateArray(String columnLabel, Array x) throws SQLException {
    throwExceptionDelegate.updateArray(columnLabel, x);
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnIndex, x, length);
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnIndex, x, length);
  }

  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnIndex, x);
  }

  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnLabel, x, length);
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnLabel, x, length);
  }

  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throwExceptionDelegate.updateAsciiStream(columnLabel, x);
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throwExceptionDelegate.updateBigDecimal(columnIndex, x);
  }

  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throwExceptionDelegate.updateBigDecimal(columnLabel, x);
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnIndex, x, length);
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnIndex, x, length);
  }

  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnIndex, x);
  }

  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnLabel, x, length);
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnLabel, x, length);
  }

  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throwExceptionDelegate.updateBinaryStream(columnLabel, x);
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throwExceptionDelegate.updateBlob(columnIndex, x);
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throwExceptionDelegate.updateBlob(columnIndex, inputStream, length);
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throwExceptionDelegate.updateBlob(columnIndex, inputStream);
  }

  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throwExceptionDelegate.updateBlob(columnLabel, x);
  }

  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throwExceptionDelegate.updateBlob(columnLabel, inputStream, length);
  }

  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throwExceptionDelegate.updateBlob(columnLabel, inputStream);
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throwExceptionDelegate.updateBoolean(columnIndex, x);
  }

  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throwExceptionDelegate.updateBoolean(columnLabel, x);
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throwExceptionDelegate.updateByte(columnIndex, x);
  }

  public void updateByte(String columnLabel, byte x) throws SQLException {
    throwExceptionDelegate.updateByte(columnLabel, x);
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throwExceptionDelegate.updateBytes(columnIndex, x);
  }

  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throwExceptionDelegate.updateBytes(columnLabel, x);
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnIndex, x, length);
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnIndex, x, length);
  }

  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnIndex, x);
  }

  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnLabel, reader, length);
  }

  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnLabel, reader, length);
  }

  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throwExceptionDelegate.updateCharacterStream(columnLabel, reader);
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throwExceptionDelegate.updateClob(columnIndex, x);
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateClob(columnIndex, reader, length);
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throwExceptionDelegate.updateClob(columnIndex, reader);
  }

  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throwExceptionDelegate.updateClob(columnLabel, x);
  }

  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateClob(columnLabel, reader, length);
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throwExceptionDelegate.updateClob(columnLabel, reader);
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throwExceptionDelegate.updateDate(columnIndex, x);
  }

  public void updateDate(String columnLabel, Date x) throws SQLException {
    throwExceptionDelegate.updateDate(columnLabel, x);
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throwExceptionDelegate.updateDouble(columnIndex, x);
  }

  public void updateDouble(String columnLabel, double x) throws SQLException {
    throwExceptionDelegate.updateDouble(columnLabel, x);
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throwExceptionDelegate.updateFloat(columnIndex, x);
  }

  public void updateFloat(String columnLabel, float x) throws SQLException {
    throwExceptionDelegate.updateFloat(columnLabel, x);
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throwExceptionDelegate.updateInt(columnIndex, x);
  }

  public void updateInt(String columnLabel, int x) throws SQLException {
    throwExceptionDelegate.updateInt(columnLabel, x);
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throwExceptionDelegate.updateLong(columnIndex, x);
  }

  public void updateLong(String columnLabel, long x) throws SQLException {
    throwExceptionDelegate.updateLong(columnLabel, x);
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throwExceptionDelegate.updateNCharacterStream(columnIndex, x, length);
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throwExceptionDelegate.updateNCharacterStream(columnIndex, x);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateNCharacterStream(columnLabel, reader, length);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throwExceptionDelegate.updateNCharacterStream(columnLabel, reader);
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throwExceptionDelegate.updateNClob(columnIndex, nClob);
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateNClob(columnIndex, reader, length);
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throwExceptionDelegate.updateNClob(columnIndex, reader);
  }

  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throwExceptionDelegate.updateNClob(columnLabel, nClob);
  }

  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throwExceptionDelegate.updateNClob(columnLabel, reader, length);
  }

  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throwExceptionDelegate.updateNClob(columnLabel, reader);
  }

  public void updateNString(int columnIndex, String nString) throws SQLException {
    throwExceptionDelegate.updateNString(columnIndex, nString);
  }

  public void updateNString(String columnLabel, String nString) throws SQLException {
    throwExceptionDelegate.updateNString(columnLabel, nString);
  }

  public void updateNull(int columnIndex) throws SQLException {
    throwExceptionDelegate.updateNull(columnIndex);
  }

  public void updateNull(String columnLabel) throws SQLException {
    throwExceptionDelegate.updateNull(columnLabel);
  }

  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throwExceptionDelegate.updateObject(columnIndex, x, scaleOrLength);
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throwExceptionDelegate.updateObject(columnIndex, x);
  }

  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throwExceptionDelegate.updateObject(columnLabel, x, scaleOrLength);
  }

  public void updateObject(String columnLabel, Object x) throws SQLException {
    throwExceptionDelegate.updateObject(columnLabel, x);
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throwExceptionDelegate.updateRef(columnIndex, x);
  }

  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throwExceptionDelegate.updateRef(columnLabel, x);
  }

  public void updateRow() throws SQLException {
    throwExceptionDelegate.updateRow();
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throwExceptionDelegate.updateRowId(columnIndex, x);
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throwExceptionDelegate.updateRowId(columnLabel, x);
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throwExceptionDelegate.updateShort(columnIndex, x);
  }

  public void updateShort(String columnLabel, short x) throws SQLException {
    throwExceptionDelegate.updateShort(columnLabel, x);
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throwExceptionDelegate.updateSQLXML(columnIndex, xmlObject);
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throwExceptionDelegate.updateSQLXML(columnLabel, xmlObject);
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throwExceptionDelegate.updateString(columnIndex, x);
  }

  public void updateString(String columnLabel, String x) throws SQLException {
    throwExceptionDelegate.updateString(columnLabel, x);
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throwExceptionDelegate.updateTime(columnIndex, x);
  }

  public void updateTime(String columnLabel, Time x) throws SQLException {
    throwExceptionDelegate.updateTime(columnLabel, x);
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throwExceptionDelegate.updateTimestamp(columnIndex, x);
  }

  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throwExceptionDelegate.updateTimestamp(columnLabel, x);
  }

  public boolean wasNull() throws SQLException {
    return throwExceptionDelegate.wasNull();
  }

  // java 7

//  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
//    return throwExceptionDelegate.getObject(columnIndex, type);
//  }
//
//  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
//    return throwExceptionDelegate.getObject(columnLabel, type);
//  }
}
