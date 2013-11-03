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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class CreateBlurServerSetupHtmlPage {

  public static void main(String[] args) throws IOException {
    InputStream inputStream = CreateBlurServerSetupHtmlPage.class.getResourceAsStream("/blur-default.properties");
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line;
    String prevLine = null;

    String key = "|||General-Server-Properties|||";
    Map<String, StringBuffer> map = new HashMap<String, StringBuffer>();
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.equals("### Shard Server Configuration")) {
        key = "|||Shard-Server-Properties|||";
      } else if (line.equals("### Controller Server Configuration")) {
        key = "|||Controller-Server-Properties|||";
      }
      if (!line.startsWith("#") && !line.isEmpty()) {
        System.out.println(prevLine);
        System.out.println(line);
        String desc = getDesc(prevLine);
        String name = getName(line);
        String value = getValue(line);
        StringBuffer buffer = map.get(key);
        if (buffer == null) {
          buffer = new StringBuffer();
          map.put(key, buffer);
        }
        buffer.append("<tr><td>").append(name);
        if (!value.trim().isEmpty()) {
          buffer.append(" (").append(value).append(")");
        }
        buffer.append("</td><td>").append(desc).append("</td></tr>");
      }
      prevLine = line;
    }
    reader.close();
    String source = args[0];
    String dest = args[1];
    replaceValuesInFile(source, dest, map);
  }

  private static void replaceValuesInFile(String s, String o, Map<String, StringBuffer> replacements)
      throws IOException {

    File source = new File(s);
    File output = new File(o);

    PrintWriter writer = new PrintWriter(output);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)));
    String line;
    while ((line = reader.readLine()) != null) {
      StringBuffer newData = replacements.get(line);
      if (newData != null) {
        writer.println(newData.toString());
      } else {
        writer.println(line);
      }
    }
    writer.close();
    reader.close();

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

}
