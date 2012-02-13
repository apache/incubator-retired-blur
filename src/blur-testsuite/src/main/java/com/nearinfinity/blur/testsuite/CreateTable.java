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
    final String tableName = args[1];
    final TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.analyzerDefinition = new AnalyzerDefinition();
    tableDescriptor.cluster = "default";
    tableDescriptor.name = tableName;
    tableDescriptor.readOnly = false;
    tableDescriptor.shardCount = 7;
    tableDescriptor.tableUri = "hdfs://localhost/blur/tables/" + tableName;
    BlurClientManager.execute(args[0], new BlurCommand<Void>() {
      @Override
      public Void call(Client client) throws BlurException, TException {
        client.createTable(tableDescriptor);
        return null;
      }
    });
  }
}
