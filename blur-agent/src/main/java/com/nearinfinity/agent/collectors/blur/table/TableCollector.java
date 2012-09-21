package com.nearinfinity.agent.collectors.blur.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.nearinfinity.agent.connections.interfaces.BlurDatabaseInterface;
import com.nearinfinity.agent.connections.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;
import com.nearinfinity.agent.types.TableMap;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class TableCollector implements Runnable {
  private static final Log log = LogFactory.getLog(TableCollector.class);

  private final Iface blurConnection;
  private final String zookeeper;
  private final BlurDatabaseInterface blurDatabase;
  private final TableDatabaseInterface database;

  public TableCollector(Iface connection, String zookeeperName, BlurDatabaseInterface blurDatabase,
      TableDatabaseInterface database) {
    this.blurConnection = connection;
    this.zookeeper = zookeeperName;
    this.blurDatabase = blurDatabase;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      /* Retrieve the zookeeper id */
      String zookeeperId;
      try {
        zookeeperId = this.blurDatabase.getZookeeperId(this.zookeeper);
      } catch (ZookeeperNameMissingException e) {
        log.error(e.getMessage(), e);
        return;
      } catch (ZookeeperNameCollisionException e) {
        log.error(e.getMessage(), e);
        return;
      }

      /* Retrieve the clusters and their info */
      for (Map<String, Object> cluster : this.blurDatabase.getClusters(zookeeperId)) {
        String clusterName = (String) cluster.get("NAME");
        Integer clusterId = (Integer) cluster.get("ID");
        List<String> tables;
        try {
          tables = blurConnection.tableListByCluster(clusterName);
        } catch (Exception e) {
          log.error("An error occured while trying to retrieve the table list for cluster["
              + clusterName + "], skipping cluster", e);
          continue;
        }

        /* Retrieve the tables and their info */
        for (final String tableName : tables) {
          TableDescriptor descriptor;
          try {
            descriptor = blurConnection.describe(tableName);
          } catch (Exception e) {
            log.error("An error occured while trying to describe the table [" + tableName
                + "], skipping table", e);
            continue;
          }

          /* Retrieve the table from the db, if it exists
           * then update its information */
          Map<String, Object> existingTable;
          try {
            existingTable = this.blurDatabase.getExistingTable(tableName, clusterId);
          } catch (TableMissingException e) {
            log.error(e.getMessage(), e);
            continue;
          } catch (TableCollisionException e) {
            log.error(e.getMessage(), e);
            continue;
          }
          
          /* Insert the table into the global table map */
          Map<String, Object> tableInfo = new HashMap<String, Object>();
          tableInfo.put("ID", existingTable.get("id"));
          tableInfo.put("ENABLED", descriptor.isEnabled);
          TableMap.getInstance().put(tableName + "_" + clusterName, tableInfo);
          
          /* spawn the different table info collectors */
          if (descriptor.isEnabled) {
            new Thread(new SchemaCollector(this.blurConnection, tableName, clusterId, descriptor,
                this.database)).start();
          }
          new Thread(new ServerCollector(this.blurConnection, tableName, clusterId, this.database))
              .start();
          new Thread(new StatsCollector(this.blurConnection, tableName, clusterId, this.database))
              .start();
        }
      }
    } catch (Exception e) {
      log.error("An unknown error occurred.", e);
    }
  }
}
