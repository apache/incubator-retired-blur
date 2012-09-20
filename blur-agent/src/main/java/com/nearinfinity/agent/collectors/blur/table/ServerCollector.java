package com.nearinfinity.agent.collectors.blur.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.DataAccessException;

import com.nearinfinity.agent.connections.interfaces.TableDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class ServerCollector implements Runnable {
  private static final Log log = LogFactory.getLog(ServerCollector.class);
  
  private final Iface blurConnection;
  private final String tableName;
  private final Integer clusterId;
  private final TableDatabaseInterface database;

  public ServerCollector(Iface connection, String tableName, Integer clusterId,
      TableDatabaseInterface database) {
    this.blurConnection = connection;
    this.tableName = tableName;
    this.clusterId = clusterId;
    this.database = database;
  }

  @Override
  public void run() {
    try {
      Map<String, String> shardServerLayout = blurConnection.shardServerLayout(tableName);
      if (shardServerLayout == null) {
        throw new NullReturnedException("No server layout was returned!");
      }
      Map<String, ArrayList<String>> serverLayout = getServerLayout(shardServerLayout);
      this.database.updateTableServer(tableName, clusterId,
          new ObjectMapper().writeValueAsString(serverLayout));

    } catch (BlurException e) {
      log.error("Unable to get shard server layout for table [" + tableName + "].", e);
    } catch (JsonProcessingException e) {
      log.error("Unable to convert the shard layout to json.", e);
    } catch (DataAccessException e) {
      log.error("An error occurred while writing the server to the database.", e);
    } catch (NullReturnedException e) {
      log.error(e.getMessage(), e);
    } catch (Exception e) {
      log.error("An unknown error occurred in the TableServerCollector.", e);
    }
  }

  private Map<String, ArrayList<String>> getServerLayout(Map<String, String> shardServerLayout) {
    Map<String, ArrayList<String>> formattedShard = new HashMap<String, ArrayList<String>>();
    for (String shard : shardServerLayout.keySet()) {
      String host = shardServerLayout.get(shard);
      if (formattedShard.get(host) != null) {
        formattedShard.get(host).add(shard);
      } else {
        formattedShard.put(host, new ArrayList<String>(Arrays.asList(shard)));
      }
    }
    return formattedShard;
  }
}