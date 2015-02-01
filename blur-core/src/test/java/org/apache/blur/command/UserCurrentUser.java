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
package org.apache.blur.command;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.command.commandtype.IndexReadCommandSingleTable;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;

public class UserCurrentUser extends IndexReadCommandSingleTable<BlurObject> {

  @Override
  public BlurObject execute(IndexContext context) throws IOException, InterruptedException {
    BlurObject blurObject = new BlurObject();
    User user = UserContext.getUser();
    if (user == null) {
      return blurObject;
    }
    blurObject.put("username", user.getUsername());
    Map<String, String> attributes = user.getAttributes();
    if (attributes != null) {
      BlurObject blurObjectAttributes = new BlurObject();
      blurObject.put("attributes", blurObjectAttributes);
      for (Entry<String, String> e : attributes.entrySet()) {
        blurObjectAttributes.put(e.getKey(), e.getValue());
      }
    }
    return blurObject;
  }

  @Override
  public String getName() {
    return "currentUser";
  }

}
