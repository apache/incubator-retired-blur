package org.apache.blur.shell;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Iface;

public class ListSnapshotsCommand extends Command {

  @Override
  public void doit(PrintWriter out, Iface client, String[] args)
      throws CommandException, TException, BlurException {
    if (args.length != 2) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    Map<String, List<String>> snapshotsPerShard = client.listSnapshots(tablename);
    for (Entry<String, List<String>> entry : snapshotsPerShard.entrySet()) {
      String shard = entry.getKey();
      out.print(shard + " : ");
      int count = 0;
      for (String snapshot : entry.getValue()) {
        count++;
        if (count == entry.getValue().size()) {
          out.println(snapshot);
        } else {
          out.print(snapshot + ", ");
        }
      }
    }
  }

  @Override
  public String description() {
    return "List the existing snapshots of a table";
  }

  @Override
  public String usage() {
    return "<tablename>";
  }

  @Override
  public String name() {
    return "list-snapshots";
  }

}
