package org.apache.blur.shell;

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
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;

public class WaitInSafemodeCommand extends Command {

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    String cluster;
    if (args.length != 2) {
      cluster = Main.getCluster(client, "Invalid args: " + help());
    } else {
      cluster = args[1];
    }
    boolean inSafeMode;
    boolean isNoLonger = false;
    do {
      inSafeMode = client.isInSafeMode(cluster);
      if (!inSafeMode) {
        if (isNoLonger) {
          out.println("Cluster " + cluster + " is no longer in safemode.");
        } else {
          out.println("Cluster " + cluster + " is not in safemode.");
        }
        return;
      }
      isNoLonger = true;
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } while (inSafeMode);
  }

  @Override
  public String description() {
    return "Wait for safe mode to exit.";
  }

  @Override
  public String usage() {
    return "[<clustername>]";
  }

  @Override
  public String name() {
    return "safemodewait";
  }

}
