package org.apache.blur.mapreduce;

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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BlurColumn implements Writable {

  private String name;
  private String value;

  public BlurColumn() {
  }

  public BlurColumn(String name, String value) {
    this.name = name;
    this.value = value;
  }

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

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "{name=" + name + ", value=" + value + "}";
  }
}
