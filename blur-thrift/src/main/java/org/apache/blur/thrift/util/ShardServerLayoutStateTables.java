package org.apache.blur.thrift.util;

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
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.ShardState;

public class ShardServerLayoutStateTables {

  public static void main(String[] args) throws BlurException, TException, IOException, InterruptedException {
    String connectionStr = args[0];

    Iface client = BlurClient.getClient(connectionStr);
    System.out.println(client.tableList());

    while (true) {
      System.out.println("===============");
      for (String table : client.tableList()) {
        Map<String, Map<String, ShardState>> state = client.shardServerLayoutState(table);
        for (String shard : state.keySet()) {
          Map<String, ShardState> shardMap = state.get(shard);
          for (Entry<String, ShardState> entry : shardMap.entrySet()) {
            System.out.println(shard + " " + entry.getKey() + " " + entry.getValue());
          }
        }
      }
      Thread.sleep(1000);
    }
  }
}
