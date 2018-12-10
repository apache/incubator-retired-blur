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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;

public class NodeUtil {
  private static Set<String> onlineControllers = new HashSet<String>();
  private static Map<String, Set<String>> onlineShards = new HashMap<String, Set<String>>();

  public static Map<String, Object> getControllerStatus() throws IOException, TException {
    CachingBlurClient client = Config.getCachingBlurClient();

    List<String> currentControllers = client.controllerList();

    Collection<String> newControllers = subtract(currentControllers, onlineControllers);
    Collection<String> missingControllers = subtract(onlineControllers, currentControllers);

    onlineControllers.addAll(newControllers);

    Map<String, Object> data = new HashMap<String, Object>();

    data.put("online", onlineControllers);
    data.put("offline", missingControllers);

    return data;
  }

  public static Collection<String> subtract(final Collection<String> a, final Collection<String> b) {
    final List<String> list = new ArrayList<String>();
    final Set<String> bag = new HashSet<String>(b);
    for (final String element : a) {
      if (!bag.contains(element)) {
        list.add(element);
      }
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public static List<Map<String, Object>> getClusterStatus() throws IOException, TException {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    CachingBlurClient client = Config.getCachingBlurClient();

    for (String cluster : client.shardClusterList()) {
      if (!onlineShards.containsKey(cluster)) {
        onlineShards.put(cluster, new HashSet<String>());
      }

      Set<String> onlineShardsForCluster = onlineShards.get(cluster);
      List<String> currentShards = client.shardList(cluster);

      Collection<String> newShards = subtract(currentShards, onlineShardsForCluster);
      Collection<String> missingShards = subtract(onlineShardsForCluster, currentShards);

      onlineShardsForCluster.addAll(newShards);

      Map<String, Object> clusterObj = new HashMap<String, Object>();
      clusterObj.put("name", cluster);
      clusterObj.put("online", onlineShardsForCluster);
      clusterObj.put("offline", missingShards);
      data.add(clusterObj);
    }

    return data;
  }

  public static Map<String, Object> getZookeeperStatus() throws IOException {
    String[] connections = Config.getBlurConfig().get("blur.zookeeper.connection").split(",");
    Set<String> onlineZookeepers = new HashSet<String>();
    Set<String> offlineZookeepers = new HashSet<String>();

    for (String connection : connections) {
      Socket socket = null;
      InputStream response = null;
      OutputStream question = null;
      try {
        URI parsedConnection = new URI("my://" + connection);
        String host = parsedConnection.getHost();
        int port = parsedConnection.getPort() >= 0 ? parsedConnection.getPort() : 2181;
        byte[] reqBytes = new byte[4];
        ByteBuffer req = ByteBuffer.wrap(reqBytes);
        req.putInt(ByteBuffer.wrap("ruok".getBytes()).getInt());
        socket = new Socket();
        socket.setSoLinger(false, 10);
        socket.setSoTimeout(20000);
        parsedConnection.getPort();
        socket.connect(new InetSocketAddress(host, port));

        response = socket.getInputStream();
        question = socket.getOutputStream();

        question.write(reqBytes);

        byte[] resBytes = new byte[4];

        response.read(resBytes);
        String status = new String(resBytes);
        if (status.equals("imok")) {
          onlineZookeepers.add(connection);
        } else {
          offlineZookeepers.add(connection);
        }
        socket.close();
        response.close();
        question.close();
      } catch (Exception e) {
        offlineZookeepers.add(connection);
      } finally {
        if (socket != null) {
          socket.close();
        }
        if (response != null) {
          response.close();
        }
        if (question != null) {
          question.close();
        }
      }
    }

    Map<String, Object> data = new HashMap<String, Object>();

    data.put("online", onlineZookeepers);
    data.put("offline", offlineZookeepers);

    return data;
  }
}
