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
package org.apache.blur.spark;

import java.util.Map;

import org.apache.blur.command.stream.StreamSplit;

public class BlurSparkSplit extends StreamSplit {

  private static final long serialVersionUID = -6636359986869398948L;

  private final String _host;
  private final int _port;
  private final int _timeout;

  public BlurSparkSplit(String host, int port, int timeout, String table, String shard, String classLoaderId,
      String user, Map<String, String> userAttributes) {
    super(table, shard, classLoaderId, user, userAttributes);
    _host = host;
    _port = port;
    _timeout = timeout;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public int getTimeout() {
    return _timeout;
  }

}
