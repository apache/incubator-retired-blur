package com.nearinfinity.blur.testsuite;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;

public class RemoveTable {

  public static void main(String[] args) throws BlurException, TException, IOException {
    String connectionStr = args[0];
    final String tableName = args[1];
    
    Iface client = BlurClient.getClient(connectionStr);
    client.removeTable(tableName, true);
  }
}
