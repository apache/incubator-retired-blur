package org.apache.blur.shell;

import java.io.PrintWriter;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Iface;

public class RemoveSnapshotCommand extends Command implements TableFirstArgCommand {
  @Override
  public void doit(PrintWriter out, Iface client, String[] args)
      throws CommandException, TException, BlurException {
    if (args.length != 3) {
      throw new CommandException("Invalid args: " + help());
    }
    String tablename = args[1];
    String snapshotName = args[2];
    client.removeSnapshot(tablename, snapshotName);
  }

  @Override
  public String description() {
    return "Remove a named snapshot";
  }

  @Override
  public String usage() {
    return "<tablename> <snapshotname>";
  }

  @Override
  public String name() {
    return "remove-snapshot";
  }
}
