package com.nearinfinity.blur.testsuite;

import java.io.IOException;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class CreateTable {

  public static void main(String[] args) throws BlurException, TException, IOException {
    String connectionStr = args[0];
    final String cluster = args[1];
    final String tableName = args[2];
    int shardCount = Integer.parseInt(args[3]);
    String uri = args[4];
    
    final TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.analyzerDefinition = new AnalyzerDefinition();
    tableDescriptor.cluster = cluster;
    tableDescriptor.name = tableName;
    tableDescriptor.readOnly = false;
    
    tableDescriptor.shardCount = shardCount;
    tableDescriptor.tableUri = uri;
    
    BlurClientManager.execute(connectionStr, new BlurCommand<Void>() {
      @Override
      public Void call(Client client) throws BlurException, TException {
        client.createTable(tableDescriptor);
        return null;
      }
    });
  }
}
