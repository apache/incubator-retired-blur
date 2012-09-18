package com.nearinfinity.agent.collectors.blur.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.DataAccessException;

import com.nearinfinity.agent.connections.blur.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;
import com.nearinfinity.agent.types.Column;
import com.nearinfinity.agent.types.Family;
import com.nearinfinity.blur.thrift.generated.AnalyzerDefinition;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.ColumnDefinition;
import com.nearinfinity.blur.thrift.generated.ColumnFamilyDefinition;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class SchemaCollector implements Runnable {
  private final Log log;
  private final Iface blurConnection;
  private final String tableName;
  private final Integer clusterId;
  private final TableDescriptor descriptor;
  private final TableDatabaseInterface database;

  public SchemaCollector(Iface connection, String tableName, Integer clusterId,
      TableDescriptor descriptor, TableDatabaseInterface database) {
    this.log = LogFactory.getLog(SchemaCollector.class);
    this.blurConnection = connection;
    this.tableName = tableName;
    this.clusterId = clusterId;
    this.descriptor = descriptor;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      Schema schema = null;
      schema = blurConnection.schema(tableName);
      if (schema != null && descriptor != null) {
        throw new NullReturnedException("No Schema or Descriptor Defined!");
      }

      List<Family> columnDefs = getColumnDefinitions(schema);

      AnalyzerDefinition analyzerDefinition = descriptor.getAnalyzerDefinition();
      if (analyzerDefinition != null) {
        Map<String, ColumnFamilyDefinition> columnFamilyDefinitions = analyzerDefinition
            .getColumnFamilyDefinitions();
        ColumnDefinition analyzerDefaultDefinition = analyzerDefinition.getDefaultDefinition();
        if (columnFamilyDefinitions == null) {
          for (Family family : columnDefs) {
            for (Column column : family.getColumns()) {
              if (analyzerDefaultDefinition == null) {
                column.setAnalyzer("UNKNOWN");
              } else {
                column.setAnalyzer(analyzerDefaultDefinition.getAnalyzerClassName());
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

            Map<String, ColumnDefinition> columnDefinitions = describeEntry.getValue()
                .getColumnDefinitions();
            ColumnDefinition columnDefaultDefinition = describeEntry.getValue()
                .getDefaultDefinition();
            if (columnDefinitions == null) {
              for (Column column : family.getColumns()) {
                if (columnDefaultDefinition == null && analyzerDefaultDefinition == null) {
                  column.setAnalyzer("UNKNOWN");
                } else if (columnDefaultDefinition == null) {
                  column.setAnalyzer(analyzerDefaultDefinition.getAnalyzerClassName());
                  column.setFullText(analyzerDefaultDefinition.isFullTextIndex());
                } else {
                  column.setAnalyzer(columnDefaultDefinition.getAnalyzerClassName());
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

                column.setAnalyzer(columnDescription.getValue().getAnalyzerClassName());
                column.setFullText(columnDescription.getValue().isFullTextIndex());
              }
            }
          }
        }
      }
      this.database.updateTableSchema(this.tableName, this.clusterId, new ObjectMapper()
          .writeValueAsString(columnDefs), this.descriptor.getAnalyzerDefinition()
          .getFullTextAnalyzerClassName());
    } catch (BlurException e) {
      log.error("Unable to get the shard schema for table [" + tableName + "].", e);
    } catch (JsonProcessingException e) {
      log.error("Unable to convert shard schema to json.", e);
    } catch (DataAccessException e) {
      log.error("An error occurred while writing the schema to the database.", e);
    } catch (NullReturnedException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error("An unknown error occurred in the TableSchemaCollector.", e);
    }
  }

  private List<Family> getColumnDefinitions(final Schema schema) {
    List<Family> columnDefs = new ArrayList<Family>();
    Map<String, Set<String>> columnFamilies = schema.getColumnFamilies();
    if (columnFamilies != null) {
      for (Map.Entry<String, Set<String>> schemaEntry : columnFamilies.entrySet()) {
        Family family = new Family(schemaEntry.getKey());
        for (String columnName : schemaEntry.getValue()) {
          Column column = new Column(columnName);
          column.setLive(true);
          family.getColumns().add(column);
        }
        columnDefs.add(family);
      }
    }
    return columnDefs;
  }
}
