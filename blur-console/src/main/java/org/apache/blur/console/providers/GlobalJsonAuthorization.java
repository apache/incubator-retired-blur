package org.apache.blur.console.providers;

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

import org.apache.blur.BlurConfiguration;
import org.apache.blur.console.model.User;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.util.Collections;
import java.util.Map;

public class GlobalJsonAuthorization implements IAuthorizationProvider {
  private static final Log log = LogFactory.getLog(GlobalJsonAuthorization.class);

  private Map<String, Map<String, String>> globalUserProperties;

  @Override
  public void setupProvider(BlurConfiguration config) throws Exception {

    String securityFile = config.get("blur.console.authorization.provider.globaljson.file");

    if (securityFile != null) {
      JsonFactory factory = new JsonFactory();
      ObjectMapper mapper = new ObjectMapper(factory);
      File from = new File(securityFile);
      TypeReference<Map<String, Map<String, String>>> typeRef = new TypeReference<Map<String, Map<String, String>>>() {
      };

      try {
        Map<String, Map<String, String>> properties = mapper.readValue(from, typeRef);
        globalUserProperties = Collections.unmodifiableMap(properties);
      } catch (Exception e) {
        log.error("Unable to parse security file.  Search may not work right.", e);
        globalUserProperties = null;
      }
    }
  }

  @Override
  public void setUserSecurityAttributes(User user) {
    if (user != null) {
      user.setSecurityAttributesMap(globalUserProperties);
    }
  }
}
