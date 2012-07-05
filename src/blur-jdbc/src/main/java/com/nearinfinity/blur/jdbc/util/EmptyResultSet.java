package com.nearinfinity.blur.jdbc.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurResultSet;



public class EmptyResultSet extends AbstractBlurResultSet {

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new EmptyResultSetMetaData();
    }
    
}