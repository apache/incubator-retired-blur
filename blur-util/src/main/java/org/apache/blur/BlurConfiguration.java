package org.apache.blur;

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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * The default constructor, which loads site defaults should nearly always be
 * used. Instantiating a BlurConfiguration without defaults is rarely necessary
 * and should generally be avoided.
 * 
 */
public class BlurConfiguration implements Cloneable {

  private Properties _properties = new Properties();

  /**
   * Create a BlurConfiguration including default properties.
   * 
   * @throws IOException
   */
  public BlurConfiguration() throws IOException {
    this(true);
  }

  /**
   * 
   * @param loadDefaults
   *          - false to load without default properties set.
   * @throws IOException
   */
  public BlurConfiguration(boolean loadDefaults) throws IOException {
    if (loadDefaults == true) {
      init();
    }
  }

  private void init() throws IOException {
    _properties.putAll(loadInternal("/blur-default.properties"));
    _properties.putAll(loadInternal("/blur-site.properties"));
  }

  public void load(InputStream inputStream) throws IOException {
    _properties.putAll(loadInternal(inputStream));
  }

  public void load(File path) throws IOException {
    FileInputStream inputStream = new FileInputStream(path);
    try {
      _properties.putAll(loadInternal(inputStream));
    } finally {
      inputStream.close();
    }
  }

  private Properties loadInternal(String path) throws IOException {
    InputStream inputStream = getClass().getResourceAsStream(path);
    if (inputStream == null) {
      throw new FileNotFoundException(path);
    }
    try {
      return loadInternal(inputStream);
    } finally {
      inputStream.close();
    }
  }

  private Properties loadInternal(InputStream inputStream) throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    return properties;
  }

  public Map<String, String> getProperties() {
    Map<String, String> result = new HashMap<String, String>();
    for (Entry<Object, Object> e : _properties.entrySet()) {
      result.put(e.getKey().toString(), e.getValue().toString());
    }
    return result;
  }

  public String get(String name) {
    return get(name, null);
  }

  public String getExpected(String name) {
    String val = get(name);
    if (val == null || val.trim().isEmpty()) {
      throw new IllegalArgumentException("Property [" + name + "] is missing or blank.");
    }
    return val;
  }

  public String get(String name, String defaultValue) {
    String property = _properties.getProperty(name, defaultValue);
    if (property == null || property.isEmpty()) {
      return defaultValue;
    }
    return property;
  }

  public int getInt(String name, int defaultValue) {
    return Integer.parseInt(get(name, Integer.toString(defaultValue)));
  }

  public long getLong(String name, long defaultValue) {
    return Long.parseLong(get(name, Long.toString(defaultValue)));
  }

  public short getShort(String name, short defaultValue) {
    return Short.parseShort(get(name, Short.toString(defaultValue)));
  }

  public void set(String name, String value) {
    _properties.setProperty(name, value);
  }

  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }

  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  public void setShort(String name, short value) {
    set(name, Short.toString(value));
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return Boolean.parseBoolean(get(name, Boolean.toString(defaultValue)));
  }

  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  public double getDouble(String name, double defaultValue) {
    return Double.parseDouble(get(name, Double.toString(defaultValue)));
  }

  @Override
  public BlurConfiguration clone() {
    BlurConfiguration clone;
    try {
      clone = (BlurConfiguration) super.clone();
    } catch (CloneNotSupportedException e) {
      // should never happen
      throw new RuntimeException(e);
    }
    clone._properties = (Properties) _properties.clone();
    return clone;
  }

  public void write(OutputStream outputStream) throws IOException {
    _properties.store(outputStream, null);
  }

}
