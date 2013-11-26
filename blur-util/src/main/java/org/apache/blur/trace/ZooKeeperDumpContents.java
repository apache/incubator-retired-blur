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
package org.apache.blur.trace;

import static org.apache.blur.utils.BlurConstants.*;

import java.io.IOException;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperDumpContents {

  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    BlurConfiguration configuration = new BlurConfiguration();
    String zkConnectionStr = configuration.get(BLUR_ZOOKEEPER_CONNECTION);
    ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {

      }
    });

    String parentPath = configuration.get(BLUR_ZOOKEEPER_TRACE_PATH);
    String id = args[0];

    String path = parentPath + "/" + id;
    List<String> children = zooKeeper.getChildren(path, false);
    System.out.println("[");
    boolean first = true;
    for (String c : children) {
      if (!first) {
        System.out.println(",");
      }
      Stat stat = zooKeeper.exists(path + "/" + c, false);
      byte[] data = zooKeeper.getData(path + "/" + c, false, stat);
      String string = new String(data);
      System.out.println(string);
      first = false;
    }
    System.out.println("]");
    zooKeeper.close();
  }

}
