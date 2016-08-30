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
package org.apache.blur.user;

import java.util.HashMap;
import java.util.Map;

import org.apache.blur.utils.ThreadValue;

public class UserContext {

  private static User _defaultUser;

  static {
    String username = System.getProperty("user.name");
    _defaultUser = new User(username, null);
  }

  public static void setDefaultUser(User user) {
    _defaultUser = copy(user);
  }

  public static User getDefaultUser() {
    return copy(_defaultUser);
  }

  public static User copy(User user) {
    Map<String, String> existingAttributes = user.getAttributes();
    if (existingAttributes != null) {
      Map<String, String> attributes = new HashMap<String, String>(existingAttributes);
      return new User(user.getUsername(), attributes);
    }
    return new User(user.getUsername(), null);
  }

  private static ThreadValue<User> _user = new ThreadValue<User>() {
    @Override
    protected User initialValue() {
      return getDefaultUser();
    }
  };

  public static void setUser(User user) {
    _user.set(user);
  }

  public static User getUser() {
    return _user.get();
  }

  public static void reset() {
    setUser(getDefaultUser());
  }

}
