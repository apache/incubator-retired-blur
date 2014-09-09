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

package org.apache.blur.console.util;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurQueryStatus;
import org.apache.blur.thrift.generated.Status;
import org.apache.blur.thrift.generated.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryUtil {

  public static int getCurrentQueryCount() throws IOException, TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    int count = 0;
    List<String> tableList = client.enabledTables();
    for (String table : tableList) {
      List<String> queries = client.queryStatusIdList(table);
      count += queries.size();
    }

    return count;
  }

  public static Map<String, Object> getQueries() throws TException, IOException {
    Map<String, Object> queriesInfo = new HashMap<String, Object>();

    int slow = 0;

    List<Map<String, Object>> queries = new ArrayList<Map<String, Object>>();

    CachingBlurClient client = Config.getCachingBlurClient();
    List<String> tableList = client.enabledTables();

    for (String table : tableList) {
      List<String> queriesForTable = client.queryStatusIdList(table);
      for (String id : queriesForTable) {
        BlurQueryStatus status = client.queryStatusById(table, id);

        if (Status.FOUND.equals(status.getStatus())) {
          Map<String, Object> info = new HashMap<String, Object>();
          info.put("uuid", id);
          User user = status.getUser();
          if(user != null && user.getUsername() != null) {
            info.put("user", user.getUsername());
          } else {
            info.put("user", status.getQuery().getUserContext());
          }
          info.put("query", status.getQuery().getQuery().getQuery());
          info.put("table", table);
          info.put("state", status.getState().getValue());
          info.put("percent", ((double) status.getCompleteShards()) / ((double) status.getTotalShards()) * 100);


          long startTime = status.getQuery().getStartTime();
          info.put("startTime", startTime);
          queries.add(info);

          if (System.currentTimeMillis() - startTime > 60000) {
            slow++;
          }
        }
      }
    }

    queriesInfo.put("slowQueries", slow);
    queriesInfo.put("queries", queries);

    return queriesInfo;
  }

  public static void cancelQuery(String table, String uuid) throws IOException, TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    client.cancelQuery(table, uuid);
  }
}
