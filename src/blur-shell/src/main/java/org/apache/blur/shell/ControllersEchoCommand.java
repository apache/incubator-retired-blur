package org.apache.blur.shell;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;

public class ControllersEchoCommand extends Command {

  @Override
  public void doit(PrintWriter out, Client client, String[] args) throws CommandException, TException, BlurException {
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

  @Override
  public String help() {
    return "list controllers";
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

}
