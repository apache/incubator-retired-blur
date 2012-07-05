package com.nearinfinity.blur.jdbc.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurResultSetMetaData;

public class EmptyResultSetMetaData extends AbstractBlurResultSetMetaData implements ResultSetMetaData {

    @Override
    public int getColumnCount() throws SQLException {
        return 0;
    }
    
}