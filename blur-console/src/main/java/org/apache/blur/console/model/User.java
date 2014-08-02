package org.apache.blur.console.model;

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

import java.util.Collection;
import java.util.Map;

public class User {

  public static final String ADMIN_ROLE = "admin"; // can do everything
  public static final String SEARCHER_ROLE = "searcher"; // reader + can query
  public static final String MANAGER_ROLE = "manager"; // searcher + destructive actions

  protected String name;

  protected String email;

  protected Collection<String> roles;

  protected Map<String, Map<String, String>> securityAttributesMap;

  public String getName() {
    return name;
  }

  public String getEmail() {
    return email;
  }

  public Collection<String> getRoles() {
    return roles;
  }

  public boolean hasRole(String role) {
    if(roles != null && !roles.isEmpty()) {
      if (roles.contains(ADMIN_ROLE)) {
        return true;
      }
      if(MANAGER_ROLE.equals(role) && roles.contains(MANAGER_ROLE)) {
        return true;
      }
      if(SEARCHER_ROLE.equals(role) && (roles.contains(MANAGER_ROLE) || roles.contains(SEARCHER_ROLE))) {
        return true;
      }
    }
    return false;
  }

  public void setSecurityAttributesMap(Map<String, Map<String, String>> securityAttributesMap) {
    this.securityAttributesMap = securityAttributesMap;
  }

  public Collection<String> getSecurityNames() {
    if(securityAttributesMap == null) {
      return null;
    }
    return securityAttributesMap.keySet();
  }

  public Map<String,String> getSecurityAttributes(String name) {
    if(securityAttributesMap == null || securityAttributesMap.isEmpty()) {
      return null;
    }
    if(securityAttributesMap.size() == 1) {
      return securityAttributesMap.values().iterator().next();
    }
    return securityAttributesMap.get(name);
  }

}
