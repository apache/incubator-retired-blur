package com.nearinfinity.blur.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Selector;

public class BlurUtil {
    
    public static Selector newSelector(String locationId) {
        Selector selector = new Selector();
        selector.locationId = locationId;
        return selector;
    }

    public static Row newRow(String id, ColumnFamily... columnFamilies) {
        Row row = new Row().setId(id);
        for (ColumnFamily columnFamily : columnFamilies) {
            row.addToColumnFamilies(columnFamily);
        }
        return row;
    }
    
    public static ColumnFamily newColumnFamily(String family, String id, Column... columns) {
        ColumnFamily columnFamily = new ColumnFamily().setFamily(family);
        columnFamily.putToColumns(id, newColumnSet(columns));
        return columnFamily;
    }
    
    public static Column newColumn(String name, String... values) {
        Column col = new Column().setName(name);
        for (String value : values) {
            col.addToValues(value);
        }
        return col;
    }
    
    public static Set<Column> newColumnSet(Column... columns) {
        TreeSet<Column> treeSet = new TreeSet<Column>(BlurConstants.COLUMN_COMPARATOR);
        treeSet.addAll(Arrays.asList(columns));
        return treeSet;
    }
    
    public static byte[] toBytes(Serializable serializable) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream stream = new ObjectOutputStream(outputStream);
            stream.writeObject(serializable);
            stream.close();
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static Serializable fromBytes(byte[] bs) {
        ObjectInputStream stream = null;
        try {
            stream = new ObjectInputStream(new ByteArrayInputStream(bs));
            return (Serializable) stream.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    //eat
                }
            }
        }
    }
}
