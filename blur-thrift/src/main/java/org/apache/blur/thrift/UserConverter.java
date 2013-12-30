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
package org.apache.blur.thrift;

import org.apache.blur.thrift.generated.User;

public class UserConverter {

  public static User toThriftUser(org.apache.blur.user.User user) {
    if (user == null) {
      return null;
    }
    return new User(user.getUsername(), user.getAttributes());
  }

  public static org.apache.blur.user.User toUserFromThrift(User user) {
    if (user == null) {
      return null;
    }
    return new org.apache.blur.user.User(user.getUsername(), user.getAttributes());
  }

}
