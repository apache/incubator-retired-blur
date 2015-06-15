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
package org.apache.blur.lucene.security.analysis;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.blur.lucene.security.analysis.DocumentVisibilityTokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;

public class DocumentVisibilityTokenStreamTest {
  @Test
  public void test() throws IOException {
    DocumentVisibilityTokenStream stream = new DocumentVisibilityTokenStream(new DocumentVisibility(
        "hi|(hi&cool&(c&(b|f)))|nice"));
    CharTermAttribute termAttribute = stream.addAttribute(CharTermAttribute.class);
    stream.reset();
    List<String> expectedTerms = new ArrayList<String>();
    expectedTerms.add("hi");
    expectedTerms.add("nice");
    expectedTerms.add("c&cool&hi&(b|f)");

    List<String> terms = new ArrayList<String>();
    while (stream.incrementToken()) {
      terms.add(termAttribute.toString());
    }
    stream.close();

    assertEquals(expectedTerms, terms);
  }
}