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

public class ParcelJsonTemplate {

  private static final String BLUR_VERSION = "|||BLUR-VERSION|||";

  public static void main(String[] args) throws IOException {

    String source = args[0];
    String dest = args[1];
    String blurVersion = args[2];

    replaceValuesInFile(source, dest, blurVersion);
  }

  private static void replaceValuesInFile(String s, String o, String blurVersion) throws IOException {
    File source = new File(s);
    File output = new File(o);
    System.out.println("Source[" + source.getAbsolutePath() + "]");
    System.out.println("Output[" + output.getAbsolutePath() + "]");
    PrintWriter writer = new PrintWriter(output);

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(source)));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.contains(BLUR_VERSION)) {
        writer.println(line.replace(BLUR_VERSION, blurVersion));
      } else {
        writer.println(line);
      }
    }
    writer.close();
    reader.close();
  }

}
