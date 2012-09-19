package com.nearinfinity.agent.collectors.blur.table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.nearinfinity.agent.connections.blur.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;
import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;
import com.nearinfinity.agent.types.TableMap;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public class TableCollector implements Runnable {
  private static final Log log = LogFactory.getLog(TableCollector.class);
  
  private final Iface connection;
  private final String zookeeper;
  private final TableDatabaseInterface database;

  public TableCollector(Iface connection, String zookeeperName, TableDatabaseInterface database) {
    this.connection = connection;
    this.zookeeper = zookeeperName;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      log.debug("Collecting table info");

      String zookeeperId = this.database.getZookeeperId(this.zookeeper);

      List<Map<String, Object>> clusters = this.database.getClusters(zookeeperId);

      for (Map<String, Object> cluster : clusters) {
        String clusterName = (String) cluster.get("NAME");
        Integer clusterId = (Integer) cluster.get("ID");

        List<String> tables = connection.tableListByCluster(clusterName);

        if (tables == null) {
          throw new NullReturnedException("No tables returned with cluster [" + clusterName + "]!");
        }

        for (final String tableName : tables) {
          TableDescriptor descriptor = connection.describe(tableName);

          if (descriptor == null) {
            log.error("Unable to describe table [" + tableName + "].");
            continue;
          }

          Map<String, Object> existingTable = this.database.getExistingTable(tableName, clusterId);
          Map<String, Object> tableInfo = new HashMap<String, Object>();
          tableInfo.put("ID", existingTable.get("id"));
          tableInfo.put("ENABLED", descriptor.isEnabled);
          TableMap.getInstance().put(tableName + "_" + clusterName, tableInfo);

          if (descriptor.isEnabled) {
            new Thread(new SchemaCollector(this.connection, tableName, clusterId, descriptor, this.database)).start();
          }
          new Thread(new ServerCollector(connection, tableName, clusterId, this.database)).start();
          new Thread(new StatsCollector(connection, tableName, clusterId, this.database)).start();
        }
      }
    } catch (ZookeeperNameCollisionException e) {
      log.error(e.getMessage(), e);
    } catch (ZookeeperNameMissingException e) {
      log.error(e.getMessage(), e);
    } catch (TableMissingException e) {
      log.error(e.getMessage(), e);
    } catch (TableCollisionException e) {
      log.error(e.getMessage(), e);
    } catch (NullReturnedException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error("An unknown error occurred in the CollectorManager.", e);
    }
  }
}
