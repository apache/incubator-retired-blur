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

import java.util.*;

public class RoleMapper {
  private Map<String, String> roleMapping;

  public RoleMapper(BlurConfiguration config) {
    roleMapping = new HashMap<String, String>();
    List<String> roles = Arrays.asList(User.ADMIN_ROLE, User.MANAGER_ROLE, User.SEARCHER_ROLE);
    for (String role : roles) {
      String configRoles = config.get("blur.console.authentication.roles." + role, role);
      String[] splitRoles = configRoles.split(",");
      for (String splitRole : splitRoles) {
        roleMapping.put(splitRole, role);
      }
    }
  }

  public Collection<String> mapRoles(Collection<String> roles) {
    if (roles != null) {
      Collection<String> mappedRoles = new ArrayList<String>(roles.size());
      for (String role : roles) {
        if (roleMapping.containsKey(role)) {
          mappedRoles.add(roleMapping.get(role));
        }
      }
      return mappedRoles;
    }
    return null;
  }

}
