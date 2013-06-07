package org.apache.blur.shell;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.BlurException;

public class ShardsEchoCommand extends Command {

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException, BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String cluster = args[1];
    List<String> shardServerList = client.shardServerList(cluster);
    String nodeName = getNodeName();
    for (String shards : shardServerList) {
      if (isSameServer(shards, nodeName)) {
        out.println(shards + "*");
      } else {
        out.println(shards);
      }
    }
  }

  private boolean isSameServer(String shard, String nodeName) {
    if (nodeName == null || shard == null) {
      return false;
    } else {
      int i = shard.lastIndexOf(':');
      if (i < 0) {
        return false;
      }
      if (nodeName.equals(shard.substring(0, i))) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String help() {
    return "list shards, args; clustername";
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
