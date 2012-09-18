package com.nearinfinity.agent.collectors.blur;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.nearinfinity.agent.collectors.blur.connections.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.TableCollisionException;
import com.nearinfinity.agent.exceptions.TableMissingException;
import com.nearinfinity.agent.exceptions.ZookeeperNameCollisionException;
import com.nearinfinity.agent.exceptions.ZookeeperNameMissingException;
import com.nearinfinity.agent.types.Column;
import com.nearinfinity.agent.types.Family;
import com.nearinfinity.agent.types.TableMap;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.thrift.generated.ColumnDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnFamilyDefinition;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;

public class TableCollector implements Runnable {
  private static final Log log = LogFactory.getLog(TableCollector.class);

  private final Iface connection;
  private final String zookeeper;
  private final TableDatabaseInterface database;

  public TableCollector(Iface connection, String zookeeperName,
      TableDatabaseInterface jdbc) {
    this.connection = connection;
    this.zookeeper = zookeeperName;
    this.database = jdbc;
  }

  @Override
  public void run() {
    log.debug("Collecting table info");

    String zookeeperId;
    try {
      zookeeperId = this.database.getTableId(this.zookeeper);
    } catch (ZookeeperNameCollisionException e) {
      log.error(e.getError(), e);
      return;
    } catch (ZookeeperNameMissingException e) {
      log.error(e.getError(), e);
      return;
    }

    List<Map<String, Object>> clusters = this.database.getClusters(zookeeperId);

    for (Map<String, Object> cluster : clusters) {
      String clusterName = (String) cluster.get("NAME");
      Integer clusterId = (Integer) cluster.get("ID");

      List<String> tables;
      try {
        tables = connection.tableListByCluster(clusterName);
        if (tables == null) {
          throw new Exception("No Tables Returned From Blur!");
        }
      } catch (Exception e) {
        log.error("Unable to get table list for cluster [" + clusterName
            + "].  Unable to retrieve stats for tables.", e);
        return;
      }

      for (final String table : tables) {
        TableDescriptor descriptor = null;
        try {
          descriptor = connection.describe(table);
          if (descriptor != null) {
            throw new Exception("No Descriptor Returned From Blur!");
          }
        } catch (Exception e) {
          log.error("Unable to describe table [" + table + "].", e);
          continue;
        }

        try {
          // Update the TableMap with the newest
          Map<String, Object> existingTable = this.database.getExistingTable(
              table, clusterId);
          Map<String, Object> tableInfo = new HashMap<String, Object>();
          tableInfo.put("ID", existingTable.get("id"));
          tableInfo.put("ENABLED", descriptor.isEnabled);
          TableMap.getInstance().put(table + "_" + clusterName, tableInfo);
        } catch (TableMissingException e) {
          log.error(e.getError(), e);
          return;
        } catch (TableCollisionException e) {
          log.error(e.getError(), e);
          return;
        }

        if (!descriptor.isEnabled) {
          continue;
        }

        String schemaDefinition = buildSchemaDefininition(table, descriptor);
        String serverDefinition = buildServerDefinition(table);

        String tableAnalyzer = descriptor.getAnalyzerDefinition()
            .getFullTextAnalyzerClassName();

        TableStats tableStats = null;
        Long tableBytes = null;
        Long tableQueries = null;
        Long tableRecordCount = null;
        Long tableRowCount = null;
        try {
          tableStats = connection.getTableStats(table);
          if (tableStats != null) {
            tableBytes = tableStats.getBytes();
            tableQueries = tableStats.getQueries();
            tableRecordCount = tableStats.getRecordCount();
            tableRowCount = tableStats.getRowCount();
          }
        } catch (Exception e) {
          log.error("Unable to get table stats for table [" + table + "].", e);
        }

        this.database.updateExistingTable(new Object[] { tableAnalyzer,
            schemaDefinition, serverDefinition, tableBytes, tableQueries,
            tableRecordCount, tableRowCount, table, clusterId });
      }
    }
  }

  private String buildSchemaDefininition(String table,
      TableDescriptor descriptor) {
    Schema schema = null;
    try {
      schema = connection.schema(table);
      if (schema != null && descriptor != null) {
        throw new Exception("No Schema or Descriptor Defined!");
      }
    } catch (Exception e) {
      log.error("Unable to get schema for table [" + table + "].", e);
      return "";
    }

    List<Family> columnDefs = new ArrayList<Family>();

    Map<String, Set<String>> columnFamilies = schema.getColumnFamilies();
    if (columnFamilies != null) {
      for (Map.Entry<String, Set<String>> schemaEntry : columnFamilies
          .entrySet()) {
        Family family = new Family(schemaEntry.getKey());
        for (String columnName : schemaEntry.getValue()) {
          Column column = new Column(columnName);
          column.setLive(true);
          family.getColumns().add(column);
        }
        columnDefs.add(family);
      }
    }

    AnalyzerDefinition analyzerDefinition = descriptor.getAnalyzerDefinition();
    if (analyzerDefinition != null) {
      Map<String, ColumnFamilyDefinition> columnFamilyDefinitions = analyzerDefinition
          .getColumnFamilyDefinitions();
      ColumnDefinition analyzerDefaultDefinition = analyzerDefinition
          .getDefaultDefinition();
      if (columnFamilyDefinitions == null) {
        for (Family family : columnDefs) {
          for (Column column : family.getColumns()) {
            if (analyzerDefaultDefinition == null) {
              column.setAnalyzer("UNKNOWN");
            } else {
              column.setAnalyzer(analyzerDefaultDefinition
                  .getAnalyzerClassName());
              column.setFullText(analyzerDefaultDefinition.isFullTextIndex());
            }
          }
        }
      } else {
        for (Map.Entry<String, ColumnFamilyDefinition> describeEntry : columnFamilyDefinitions
            .entrySet()) {
          Family family = new Family(describeEntry.getKey());
          int familyIndex = columnDefs.indexOf(family);

          if (familyIndex == -1) {
            columnDefs.add(family);
          } else {
            family = columnDefs.get(familyIndex);
          }

          Map<String, ColumnDefinition> columnDefinitions = describeEntry
              .getValue().getColumnDefinitions();
          ColumnDefinition columnDefaultDefinition = describeEntry.getValue()
              .getDefaultDefinition();
          if (columnDefinitions == null) {
            for (Column column : family.getColumns()) {
              if (columnDefaultDefinition == null
                  && analyzerDefaultDefinition == null) {
                column.setAnalyzer("UNKNOWN");
              } else if (columnDefaultDefinition == null) {
                column.setAnalyzer(analyzerDefaultDefinition
                    .getAnalyzerClassName());
                column.setFullText(analyzerDefaultDefinition.isFullTextIndex());
              } else {
                column.setAnalyzer(columnDefaultDefinition
                    .getAnalyzerClassName());
                column.setFullText(columnDefaultDefinition.isFullTextIndex());
              }
            }
          } else {
            for (Map.Entry<String, ColumnDefinition> columnDescription : columnDefinitions
                .entrySet()) {
              Column column = new Column(columnDescription.getKey());
              int columnIndex = family.getColumns().indexOf(column);

              if (columnIndex == -1) {
                family.getColumns().add(column);
              } else {
                column = family.getColumns().get(columnIndex);
              }

              column.setAnalyzer(columnDescription.getValue()
                  .getAnalyzerClassName());
              column
                  .setFullText(columnDescription.getValue().isFullTextIndex());
            }
          }
        }
      }
    }

    try {
      return new ObjectMapper().writeValueAsString(columnDefs);
    } catch (Exception e) {
      log.error("Unable to convert column definition to json.", e);
      return "";
    }
  }

  private String buildServerDefinition(String table) {
    Map<String, String> shardServerLayout;

    try {
      shardServerLayout = connection.shardServerLayout(table);
      if (shardServerLayout == null) {
        throw new Exception("No Server Layout Defined!");
      }
    } catch (Exception e) {
      log.error("Unable to get shard server layout for table [" + table + "].",
          e);
      return "";
    }

    Map<String, ArrayList<String>> formattedShard = new HashMap<String, ArrayList<String>>();
    for (String shard : shardServerLayout.keySet()) {
      String host = shardServerLayout.get(shard);
      if (formattedShard.get(host) != null) {
        formattedShard.get(host).add(shard);
      } else {
        formattedShard.put(host, new ArrayList<String>(Arrays.asList(shard)));
      }
    }

    try {
      return new ObjectMapper().writeValueAsString(formattedShard);
    } catch (Exception e) {
      log.error("Unable to convert shard server layout to json.", e);
      return "";
    }
  }
}
