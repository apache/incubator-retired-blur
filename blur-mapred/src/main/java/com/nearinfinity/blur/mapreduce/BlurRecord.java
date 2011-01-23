package com.nearinfinity.blur.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class BlurRecord implements Writable {
    
    private String id;
    private String superKey;
    private String columnFamily;
    private List<BlurColumn> columns = new ArrayList<BlurColumn>();

    @Override
    public void readFields(DataInput in) throws IOException {
        id = IOUtil.readString(in);
        superKey = IOUtil.readString(in);
        columnFamily = IOUtil.readString(in);
        int size = IOUtil.readVInt(in);
        columns.clear();
        for (int i = 0; i < size; i++) {
            BlurColumn column = new BlurColumn();
            column.readFields(in);
            columns.add(column);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        IOUtil.writeString(out, id);
        IOUtil.writeString(out, superKey);
        IOUtil.writeString(out, columnFamily);
        IOUtil.writeVInt(out, columns.size());
        for (BlurColumn column : columns) {
            column.write(out);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSuperKey() {
        return superKey;
    }

    public void setSuperKey(String superKey) {
        this.superKey = superKey;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public List<BlurColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<BlurColumn> columns) {
        this.columns = columns;
    }
    
    public void clearColumns() {
        columns.clear();
    }

    public void addColumn(BlurColumn column) {
        columns.add(column);
    }

    public void addColumn(String name, String value) {
        BlurColumn blurColumn = new BlurColumn();
        blurColumn.setName(name);
        blurColumn.setValue(value);
        addColumn(blurColumn);
    }
}
