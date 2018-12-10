/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.blur.shell;

import java.io.IOException;
import java.io.PrintWriter;

import jline.console.ConsoleReader;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class RemoveShardServerCommand extends Command {
  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    ZooKeeper zooKeeper = null;
    try {
      try {
        BlurConfiguration configuration = new BlurConfiguration();
        String connectString = configuration.get(BlurConstants.BLUR_ZOOKEEPER_CONNECTION);
        int sessionTimeout = configuration.getInt(BlurConstants.BLUR_ZOOKEEPER_TIMEOUT, 30000);
        zooKeeper = new ZooKeeperClient(connectString, sessionTimeout, new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
      } catch (IOException e) {
        if (Main.debug) {
          e.printStackTrace();
        }
        throw new CommandException(e.getMessage());
      }

      String node = args[1];

      out.println("Are you sure you want to remove the shard server [" + node + "]? [y/N]");
      out.flush();

      ConsoleReader reader = getConsoleReader();
      try {
        int readCharacter = reader.readCharacter();
        if (!(readCharacter == 'y' || readCharacter == 'Y')) {
          return;
        }
      } catch (IOException e) {
        if (Main.debug) {
          e.printStackTrace();
        }
        throw new CommandException(e.getMessage());
      }

      String path = "/blur/clusters/" + Main.getCluster(client) + "/online-nodes/" + node;
      zooKeeper.delete(path, -1);
    } catch (InterruptedException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    } catch (KeeperException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    } finally {
      if (zooKeeper != null) {
        try {
          zooKeeper.close();
        } catch (InterruptedException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  @Override
  public String description() {
    return "Remove a node from ZooKeeper which will cause the cluster to consider it dead.";
  }

  @Override
  public String usage() {
    return "<node:port>";
  }

  @Override
  public String name() {
    return "remove-shard";
  }
}
