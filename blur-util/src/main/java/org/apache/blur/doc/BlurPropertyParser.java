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
package org.apache.blur.doc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

public class BlurPropertyParser {

  public Map<String, List<BlurProp>> parse() throws IOException {
    InputStream inputStream = BlurPropertyParser.class.getResourceAsStream("/blur-default.properties");
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    String prevLine = null;

    String key = "|||General-Server-Properties|||";

    Map<String, List<BlurProp>> map = new HashMap<String, List<BlurProp>>();
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.equals("### Shard Server Configuration")) {
        key = "|||Shard-Server-Properties|||";
      } else if (line.equals("### Controller Server Configuration")) {
        key = "|||Controller-Server-Properties|||";
      }
      if (!line.startsWith("#") && !line.isEmpty()) {
        String desc = getDesc(prevLine);
        String name = getName(line);
        String value = getValue(line);
        String type = getType(value);
        List<BlurProp> props = map.get(key);
        if (props == null) {
          props = Lists.newArrayList();
          map.put(key, props);
        }
        BlurProp p = new BlurProp();
        p.setName(name);
        p.setDefaultVal(value);
        p.setDescription(desc);
        p.setType(type); // infer type...
        props.add(p);
      }
      prevLine = line;
    }
    return map;
  }

  String getType(String value) {
    if (value == null || value.isEmpty()) {
      return "string";
    }

    if (isNumeric(value)) {
      return "long";
    }

    if ("true".equals(value) || "false".equals(value)) {
      return "boolean";
    }

    if (isDouble(value)) {
      return "double";
    }
    return "string";
  }

  private boolean isDouble(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private boolean isNumeric(String value) {
    byte[] chars = value.getBytes();

    int start = 0;
    if (value.charAt(0) == '-') {
      start = 1;
    }

    for (int i = start; i < chars.length; i++) {
      if (!Character.isDigit(chars[i])) {
        return false;
      }
    }
    return true;
  }

  private static String getValue(String line) {
    int index = line.indexOf('=');
    if (index < 0) {
      throw new RuntimeException();
    }
    return line.substring(index + 1);
  }

  private static String getName(String line) {
    int index = line.indexOf('=');
    if (index < 0) {
      throw new RuntimeException();
    }
    return line.substring(0, index);
  }

  private static String getDesc(String prevLine) {
    return prevLine.substring(1).trim();
  }

  public static class BlurProp {
    private String name;
    private String description;
    private String defaultVal;
    private String type;
    private boolean isRequired;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public String getDefaultVal() {
      return defaultVal;
    }

    public void setDefaultVal(String defaultVal) {
      this.defaultVal = defaultVal;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    // We'll have a default if it is.
    public boolean isRequired() {
      return ((defaultVal != null) && (!defaultVal.isEmpty()));
    }
  }

}
