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
package org.apache.blur;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.ThriftBlurControllerServer;
import org.apache.blur.thrift.ThriftBlurShardServer;
import org.apache.blur.thrift.ThriftServer;

public class ExternalThriftServer {

  public static void main(String[] args) throws Exception {

    // Properties properties = System.getProperties();
    // Set<Object> keySet = properties.keySet();
    // Map<String, String> map = new TreeMap<String, String>();
    // for (Object o : keySet) {
    // Object object = properties.get(o);
    // map.put(o.toString(), object.toString());
    // }
    // for (Entry<String, String> e : map.entrySet()) {
    // System.out.println(e.getKey() + " " + e.getValue());
    // }

    String type = args[0];
    Integer serverIndex = Integer.parseInt(args[1]);
    String configPath = args[2];
    File path = new File(configPath);
    System.out.println("Loading config from [" + path + "]");
    BlurConfiguration configuration = new BlurConfiguration(false);
    configuration.load(path);
    System.out.println("Configuration Loaded");
    Map<String, String> properties = new TreeMap<String, String>(configuration.getProperties());
    for (Entry<String, String> e : properties.entrySet()) {
      System.out.println(e.getKey() + "=>" + e.getValue());
    }

    final ThriftServer thriftServer;
    if (type.equals("shard")) {
      thriftServer = ThriftBlurShardServer.createServer(serverIndex, configuration);
    } else if (type.equals("controller")) {
      thriftServer = ThriftBlurControllerServer.createServer(serverIndex, configuration);
    } else {
      throw new RuntimeException("Unknown type [" + type + "]");
    }
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          thriftServer.start();
        } catch (TTransportException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }).start();
    while (true) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      int localPort = ThriftServer.getBindingPort(thriftServer.getServerTransport());
      if (localPort == 0) {
        continue;
      } else {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        break;
      }
    }
    System.out.println("ONLINE");
  }
}
