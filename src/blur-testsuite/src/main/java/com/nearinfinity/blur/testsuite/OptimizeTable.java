package com.nearinfinity.blur.testsuite;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;

public class OptimizeTable {

  public static void main(String[] args) throws BlurException, TException, IOException {
    final String tableName = args[1];
    final int segmentCount = Integer.parseInt(args[2]);
    Iface client = BlurClient.getClient(args[0]);
    client.optimize(tableName, segmentCount);
  }
}
