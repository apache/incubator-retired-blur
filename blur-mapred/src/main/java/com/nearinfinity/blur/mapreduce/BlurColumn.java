package com.nearinfinity.blur.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class BlurColumn implements Writable {
    
    private String name;
    private String value;
    
    public boolean hasNull() {
        if (name == null || value == null) {
            return true;
        }
        return false;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = IOUtil.readString(in);
        value = IOUtil.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        IOUtil.writeString(out, name);
        IOUtil.writeString(out, value);
    }
}
