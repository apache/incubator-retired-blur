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
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Splitter;

public class JsonPropertyFormatter {

  private static final String BLUR = "blur";
  private static final String DEFAULT = "default";
  private static final String TYPE = "type";
  private static final String CONFIGURABLE_IN_WIZARD = "configurableInWizard";
  private static final String REQUIRED = "required";
  private static final String CONFIG_NAME = "configName";
  private static final String DESCRIPTION = "description";
  private static final String LABEL = "label";
  private static final String NAME = "name";

  public String separator() {
    return ",";
  }

  public String format(BlurProp prop) {
    JSONObject jsonObject = new JSONObject();
    try {
      jsonObject.put(NAME, prop.getName().replace(".", "_"));
      jsonObject.put(LABEL, pretty(prop.getName()));
      jsonObject.put(DESCRIPTION, prop.getDescription());
      jsonObject.put(CONFIG_NAME, prop.getName());
      jsonObject.put(REQUIRED, prop.isRequired());
      jsonObject.put(CONFIGURABLE_IN_WIZARD, prop.isRequired());
      jsonObject.put(TYPE, prop.getType());
      jsonObject.put(DEFAULT, prop.getDefaultVal());
      return jsonObject.toString(1);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private String pretty(String s) {
    Splitter splitter = Splitter.on('.');
    StringBuilder builder = new StringBuilder();
    for (String split : splitter.split(s)) {
      if (builder.length() == 0 && split.equals(BLUR)) {
        // skip
      } else {
        if (builder.length() != 0) {
          builder.append(' ');
        }
        builder.append(split.substring(0, 1).toUpperCase()).append(split.substring(1));
      }
    }
    return builder.toString();
  }
}
