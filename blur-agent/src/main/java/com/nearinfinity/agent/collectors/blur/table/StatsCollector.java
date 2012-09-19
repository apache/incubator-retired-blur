package com.nearinfinity.agent.collectors.blur.table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;

import com.nearinfinity.agent.connections.blur.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class StatsCollector implements Runnable {
  private static final Log log = LogFactory.getLog(StatsCollector.class);
  
  private final Iface blurConnection;
  private final String tableName;
  private final Integer clusterId;
  private final TableDatabaseInterface database;

  public StatsCollector(Iface connection, String tableName, Integer clusterId,
      TableDatabaseInterface database) {
    this.blurConnection = connection;
    this.tableName = tableName;
    this.clusterId = clusterId;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      TableStats tableStats = blurConnection.getTableStats(tableName);

      if (tableStats == null) {
        throw new NullReturnedException("No table statistics were returned!");
      }

      this.database.updateTableStats(tableName, clusterId, tableStats.getBytes(),
          tableStats.getQueries(), tableStats.getRecordCount(), tableStats.getRowCount());
    } catch (BlurException e) {
      log.error("Unable to get table stats for table [" + tableName + "].", e);
    } catch (DataAccessException e) {
      log.error("An error occurred while writing the server to the database.", e);
    } catch (NullReturnedException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error("An unknown error occurred in the TableServerCollector.", e);
    }
  }
}