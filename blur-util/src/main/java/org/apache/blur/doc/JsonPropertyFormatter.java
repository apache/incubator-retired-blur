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

import org.apache.blur.doc.BlurPropertyParser.BlurProp;

public class JsonPropertyFormatter {

  public String separator() {
    return ",";
  }

  public String format(BlurProp prop) {
    return "    {\n" +
        "      \"name\":\"" + prop.getName().replace(".", "_") + "\",\n" +
        "      \"label\":\"" + prop.getName().replace(".", " ") + "\",\n" +
        "      \"description\": \"" + escape(prop.getDescription()) + "\",\n" +
        "      \"configName\":\"" + prop.getName() + "\",\n" +
        "      \"required\":\"" + prop.isRequired() + "\",\n" +
        "      \"type\":\"" + prop.getType() + "\",\n" +
        "      \"default\":\"" + prop.getDefaultVal() + "\",\n" +
        "      \"configurableInWizard\":true\n" +
        "    }\n";
  }

  private String escape(String value) {
    return value.replace("\"", "'");
  }
}
