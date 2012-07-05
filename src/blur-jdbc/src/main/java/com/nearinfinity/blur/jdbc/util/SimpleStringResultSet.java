package com.nearinfinity.blur.jdbc.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurResultSet;
import com.nearinfinity.blur.jdbc.abstractimpl.AbstractBlurResultSetMetaData;

public class SimpleStringResultSet extends AbstractBlurResultSet {
    
    private List<String> columnNames;
    private List<Map<String, String>> data;
    private int position = -1;
    private String lastValue;

    public SimpleStringResultSet(List<String> columnNames, List<Map<String,String>> data) {
        this.columnNames = columnNames;
        this.data = data;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new SimpleStringResultSetMetaData(columnNames);
    }

    @Override
    public boolean next() throws SQLException {
        if (position + 1 >= data.size()) {
            return false;
        }
        position++;
        return true;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        String name = columnNames.get(columnIndex-1);
        Map<String, String> row = data.get(position);
        return lastValue = row.get(name);
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        return lastValue == null ? true : false;
    }
    
    @Override
    public void close() throws SQLException {
        
    }
    
    public static class SimpleStringResultSetMetaData extends AbstractBlurResultSetMetaData {

        private List<String> columnNames;

        public SimpleStringResultSetMetaData(List<String> columnNames) {
            this.columnNames = columnNames;
        }

        @Override
        public int getColumnCount() throws SQLException {
            return columnNames.size();
        }

        @Override
        public String getColumnName(int column) throws SQLException {
            return columnNames.get(column - 1);
        }

        @Override
        public int getColumnType(int column) throws SQLException {
            return Types.VARCHAR;
        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
            return "string";
        }
        
    }
}
