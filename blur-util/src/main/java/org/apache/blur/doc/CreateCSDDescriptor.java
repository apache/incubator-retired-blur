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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.blur.doc.BlurPropertyParser.BlurProp;

public class CreateCSDDescriptor {

  public static void main(String[] args) throws IOException {
    BlurPropertyParser parser = new BlurPropertyParser();
    Map<String, List<BlurProp>> props = parser.parse();
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

    replaceValuesInFile(source, dest, map);
  }

  private static void replaceValuesInFile(String s, String o, Map<String, StringBuffer> replacements)
      throws IOException {

    File source = new File(s);
    File output = new File(o);
    System.out.println("Source[" + source.getAbsolutePath() + "]");
    System.out.println("Output[" + output.getAbsolutePath() + "]");
    PrintWriter writer = new PrintWriter(output);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)));
    String line;
    while ((line = reader.readLine()) != null) {
      StringBuffer newData = replacements.get(line.trim());

      if (newData != null) {
        writer.println(newData.toString());
      } else {
        writer.println(line);
      }
    }
    writer.close();
    reader.close();
  }

}
