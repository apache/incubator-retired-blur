/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;



public class BlurColumn implements Writable {
    
    private String name;
    private List<String> values;
    
    public boolean hasNull() {
        if (name == null || values == null) {
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

    @Override
    public void readFields(DataInput in) throws IOException {
        name = IOUtil.readString(in);
        int length = IOUtil.readVInt(in);
        values = new ArrayList<String>(length);
        for (int i = 0; i < length; i++) {
            values.add(IOUtil.readString(in));
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        IOUtil.writeString(out, name);
        int length = values.size();
        IOUtil.writeVInt(out, length);
        for (int i = 0; i < length; i++) {
            IOUtil.writeString(out, values.get(i));
        }
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
