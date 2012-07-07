package com.nearinfinity.blur.testsuite;

import static com.nearinfinity.blur.utils.BlurUtil.newColumn;
import static com.nearinfinity.blur.utils.BlurUtil.newRecordMutation;

import java.util.UUID;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClient;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class RapidlyCreateAndDeleteTables {

  public static void main(String[] args) throws BlurException, TException {
    String connectionStr = args[0];
    final String cluster = args[1];
    String uri = args[2];
    int shardCount = 1;
    Iface client = BlurClient.getClient(connectionStr);
    while (true) {
      String tableName = UUID.randomUUID().toString();
      System.out.println("Creating [" + tableName + "]");
      createTable(client, cluster, uri, shardCount, tableName);
      System.out.println("Loading [" + tableName + "]");
      loadTable(client, tableName);
      System.out.println("Disabling [" + tableName + "]");
      disable(client, tableName);
      System.out.println("Removing [" + tableName + "]");
      delete(client, tableName);
    }
  }

  private static void disable(Iface client, String tableName) throws BlurException, TException {
    client.disableTable(tableName);
  }

  private static void delete(Iface client, String tableName) throws BlurException, TException {
    client.removeTable(tableName, true);
  }

  private static void loadTable(Iface client, String tableName) throws BlurException, TException {
    RowMutation mutation = new RowMutation();
    mutation.table = tableName;
    mutation.waitToBeVisible = true;
    mutation.rowId = "test";
    mutation.addToRecordMutations(newRecordMutation("test", "test", newColumn("test", "test")));
    mutation.rowMutationType = RowMutationType.REPLACE_ROW;
    client.mutate(mutation);
  }

  private static void createTable(Iface client, final String cluster, String uri, int shardCount, String tableName) throws BlurException, TException {
    final TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.analyzerDefinition = new AnalyzerDefinition();
    tableDescriptor.cluster = cluster;

    tableDescriptor.name = tableName;
    tableDescriptor.readOnly = false;

    tableDescriptor.shardCount = shardCount;
    tableDescriptor.tableUri = uri + "/" + tableName;

    client.createTable(tableDescriptor);
  }

}
