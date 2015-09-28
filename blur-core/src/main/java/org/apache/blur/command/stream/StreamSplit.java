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
package org.apache.blur.command.stream;

import java.io.Serializable;
import java.util.Map;

public class StreamSplit implements Serializable {

  private static final long serialVersionUID = -1760098859541747672L;

  private final String table;
  private final String shard;
  private final String classLoaderId;
  private final String user;
  private final Map<String, String> userAttributes;

  public StreamSplit(String table, String shard, String classLoaderId, String user, Map<String, String> userAttributes) {
    this.table = table;
    this.shard = shard;
    this.classLoaderId = classLoaderId;
    this.user = user;
    this.userAttributes = userAttributes;
  }

  public String getTable() {
    return table;
  }

  public String getShard() {
    return shard;
  }

  public String getClassLoaderId() {
    return classLoaderId;
  }

  public String getUser() {
    return user;
  }

  public Map<String, String> getUserAttributes() {
    return userAttributes;
  }

  public StreamSplit copy() {
    return new StreamSplit(table, shard, classLoaderId, user, userAttributes);
  }

}
