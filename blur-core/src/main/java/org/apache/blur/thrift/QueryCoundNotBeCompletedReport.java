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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.thrift.generated.BlurQuery;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryCoundNotBeCompletedReport {

  private final String _message;

  public QueryCoundNotBeCompletedReport(Map<String, String> tableLayout, int shardCount, Map<String, Long> shardInfo,
      BlurQuery blurQuery) throws JSONException {
    String missingShardsFromResult = "\n";
    if (shardCount != shardInfo.size()) {

      Set<String> missingShardResults = new TreeSet<String>();
      Set<String> servers = new TreeSet<String>();
      for (Entry<String, String> e : tableLayout.entrySet()) {
        String shard = e.getKey();
        String server = e.getValue();
        if (!shardInfo.containsKey(shard)) {
          missingShardResults.add(shard);
          servers.add(server);
        }
      }
      missingShardsFromResult = "\nThere appears to be some missing shards from the results.\n";
      missingShardsFromResult += "Missing Shards:\n\t" + missingShardResults + "\n";
      missingShardsFromResult += "From Servers:\n\t" + servers + "\n";
    }

    JSONObject jsonObject = new JSONObject();
    jsonObject.put("tableLayout", getTableLayout(tableLayout));
    jsonObject.put("tableShardCount", shardCount);
    jsonObject.put("queryShardCount", shardInfo.size());
    jsonObject.put("queryResultCount", getShardInfo(shardInfo));
    _message = missingShardsFromResult + jsonObject.toString(1);
  }

  private JSONObject getShardInfo(Map<String, Long> shardInfo) throws JSONException {
    JSONObject j = new JSONObject();
    for (Entry<String, Long> e : shardInfo.entrySet()) {
      j.put(e.getKey(), e.getValue());
    }
    return j;
  }

  private JSONObject getTableLayout(Map<String, String> tableLayout) throws JSONException {
    JSONObject j = new JSONObject();
    for (Entry<String, String> e : tableLayout.entrySet()) {
      j.put(e.getKey(), e.getValue());
    }
    return j;
  }

  @Override
  public String toString() {
    return _message;
  }

}
