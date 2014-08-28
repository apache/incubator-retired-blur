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
package org.apache.blur.manager.command;

import java.util.Map;

public class Response {

  private final Map<Shard, Object> _shardResults;
  private final Map<Server, Object> _serverResults;
  private final Object _serverResult;
  private final boolean _aggregatedResults;

  private Response(Map<Shard, Object> shardResults, Object serverResult, Map<Server, Object> serverResults,
      boolean aggregatedResults) {
    _shardResults = shardResults;
    _serverResult = serverResult;
    _aggregatedResults = aggregatedResults;
    _serverResults = serverResults;
  }

  public boolean isAggregatedResults() {
    return _aggregatedResults;
  }

  public Map<Shard, Object> getShardResults() {
    return _shardResults;
  }

  public Object getServerResult() {
    return _serverResult;
  }

  public Map<Server, Object> getServerResults() {
    return _serverResults;
  }

  public static Response createNewAggregateResponse(Object object) {
    return new Response(null, object, null, true);
  }

  public static Response createNewShardResponse(Map<Shard, Object> map) {
    return new Response(map, null, null, false);
  }

  public static Response createNewServerResponse(Map<Server, Object> result) {
    return new Response(null, null, result, true);
  }
}
