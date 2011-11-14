package com.nearinfinity.blur;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BlurConfiguration {

  private Properties _properties = new Properties();

  public static void main(String[] args) throws IOException {
    BlurConfiguration configuration = new BlurConfiguration();
    System.out.println(configuration.get("test"));
    System.out.println(configuration.get("test2"));
    System.out.println(configuration.get("test3", "def"));
  }

  public BlurConfiguration() throws IOException {
    init();
  }

  private void init() throws IOException {
    _properties.putAll(load("/blur-default.properties"));
    _properties.putAll(load("/blur-site.properties"));
  }

  private Properties load(String path) throws IOException {
    InputStream inputStream = getClass().getResourceAsStream(path);
    if (inputStream == null) {
      throw new FileNotFoundException(path);
    }
    Properties properties = new Properties();
    properties.load(inputStream);
    return properties;
  }

  public String get(String name) {
    return get(name, null);
  }

  public String get(String name, String defaultValue) {
    return _properties.getProperty(name, defaultValue);
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

}
