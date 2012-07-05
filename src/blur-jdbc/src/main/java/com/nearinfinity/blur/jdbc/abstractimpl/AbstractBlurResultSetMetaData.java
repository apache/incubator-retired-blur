package com.nearinfinity.blur.jdbc.abstractimpl;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.nearinfinity.blur.jdbc.util.NotImplemented;

public abstract class AbstractBlurResultSetMetaData implements ResultSetMetaData {
    
    private ResultSetMetaData throwExceptionDelegate;

    public AbstractBlurResultSetMetaData() {
        throwExceptionDelegate = (ResultSetMetaData) Proxy.newProxyInstance(ResultSetMetaData.class.getClassLoader(),
            new Class[] { ResultSetMetaData.class },
            new InvocationHandler() {
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