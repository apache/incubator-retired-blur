package com.nearinfinity.blur.testsuite;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;

public class OptimizeTable {

  public static void main(String[] args) throws BlurException, TException, IOException {
    final String tableName = args[1];
    final int segmentCount = Integer.parseInt(args[2]);
    BlurClientManager.execute(args[0], new BlurCommand<Void>() {
      @Override
      public Void call(Client client) throws BlurException, TException {
        client.optimize(tableName, segmentCount);
        return null;
      }
    });
  }
}
