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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;

public class ControllersEchoCommand extends Command {

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    List<String> controllerServerList = client.controllerServerList();
    String nodeName = getNodeName();
    for (String controller : controllerServerList) {
      if (isSameServer(controller, nodeName)) {
        out.println(controller + "*");
      } else {
        out.println(controller);
      }
    }
  }

  private boolean isSameServer(String controller, String nodeName) {
    if (nodeName == null || controller == null) {
      return false;
    } else {
      int i = controller.lastIndexOf(':');
      if (i < 0) {
        return false;
      }
      if (nodeName.equals(controller.substring(0, i))) {
        return true;
      }
    }
    return false;
  }

  public static String getNodeName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      String message = e.getMessage();
      int index = message.indexOf(':');
      if (index < 0) {
        return null;
      }
      String nodeName = message.substring(0, index);
      return nodeName;
    }
  }

  @Override
  public String description() {
    return "List controllers.";
  }

  @Override
  public String usage() {
    return "";
  }

  @Override
  public String name() {
    return "controllers";
  }

}
