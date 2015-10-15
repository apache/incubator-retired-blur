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
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.doc.BlurPropertyParser.BlurProp;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class CreateCSDDescriptor {

  private static final String BLUR_VERSION = "|||BLUR-VERSION|||";

  public static void main(String[] args) throws IOException, JSONException {
    BlurPropertyParser parser = new BlurPropertyParser();
    Map<String, List<BlurProp>> props = parser.parse();
    removeProps("blur.shard.block.cache.total.size", props);
    Map<String, StringBuffer> map = new HashMap<String, StringBuffer>();

    JsonPropertyFormatter formatter = new JsonPropertyFormatter();

    for (Map.Entry<String, List<BlurProp>> prop : props.entrySet()) {

      StringBuffer buffer = map.get(prop.getKey());
      if (buffer == null) {
        buffer = new StringBuffer();
        map.put(prop.getKey(), buffer);
      }
      boolean first = true;

      for (BlurProp p : prop.getValue()) {
        if (!first) {
          buffer.append(formatter.separator());
        }
        buffer.append(formatter.format(p));
        first = false;
      }
    }

    String source = args[0];
    String dest = args[1];
    String blurVersion = args[2];

    replaceValuesInFile(source, dest, map, formatVersion(blurVersion));

    FileInputStream input = new FileInputStream(dest);
    try {
      // Check if the csd is a good json file.
      new JSONObject(IOUtils.toString(input));
    } finally {
      input.close();
    }
  }

  private static void removeProps(String propertyName, Map<String, List<BlurProp>> props) {
    for (Entry<String, List<BlurProp>> e : props.entrySet()) {
      List<BlurProp> value = e.getValue();
      List<BlurProp> remove = new ArrayList<BlurProp>();
      for (BlurProp blurProp : value) {
        if (propertyName.equals(blurProp.getName())) {
          remove.add(blurProp);
        }
      }
      value.removeAll(remove);
    }
  }

  public static String formatVersion(String blurVersion) {
    return blurVersion.replace("-", ".");
  }

  private static void replaceValuesInFile(String s, String o, Map<String, StringBuffer> replacements, String blurVersion)
      throws IOException {

    File source = new File(s);
    File output = new File(o);
    output.getParentFile().mkdirs();
    System.out.println("Source[" + source.getAbsolutePath() + "]");
    System.out.println("Output[" + output.getAbsolutePath() + "]");
    PrintWriter writer = new PrintWriter(output);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)));
    String line;
    while ((line = reader.readLine()) != null) {
      StringBuffer newData = replacements.get(line.trim());
      if (line.contains(BLUR_VERSION)) {
        writer.println(line.replace(BLUR_VERSION, blurVersion));
      } else if (newData != null) {
        writer.println(newData.toString());
      } else {
        writer.println(line);
      }
    }
    writer.close();
    reader.close();
  }

}
