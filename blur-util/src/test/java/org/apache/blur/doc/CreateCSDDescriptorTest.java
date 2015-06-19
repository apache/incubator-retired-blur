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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class CreateCSDDescriptorTest {
  private File source;
  private File dest = new File("target/service.sdl");

  @Before
  public void before() {
    source = new File(CreateCSDDescriptorTest.class.getResource("/service.sdl.template").getFile());
  }

  // Not a great test but makes sure i get it mostly put back together.
  @Test
  public void testGeneration() throws IOException {
    Properties defaultProperties = new Properties();
    defaultProperties.load(CreateCSDDescriptorTest.class.getResourceAsStream("/blur-default.properties"));
    int numProps = defaultProperties.size();
    int numDocumentedProps = getDocumentedProps();

    assertEquals(numProps, numDocumentedProps);
  }

  private int getDocumentedProps() throws IOException {
    int count = 0;
    String docs = readGeneratedDocs();
    String[] props = docs.split("configurableInWizard");

    // System.out.println("PROPERTIES:");

    for (String p : props) {
      if (!p.isEmpty()) {
        count++;
        // System.out.println("PROP: " + p);
      }
    }

    return count;
  }

  private String readGeneratedDocs() throws IOException {
    StringBuffer buffer = new StringBuffer();
    FileInputStream fis = new FileInputStream(dest);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        buffer.append(line);
      }
    } finally {
      if (fis != null) {
        fis.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
    return buffer.toString();
  }

}
