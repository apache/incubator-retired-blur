package org.apache.blur.agent.collectors.blur.table;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.blur.agent.connections.blur.interfaces.TableDatabaseInterface;
import org.apache.blur.agent.exceptions.NullReturnedException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.DataAccessException;


public class ServerCollector implements Runnable {
	private static final Log log = LogFactory.getLog(ServerCollector.class);

	private final Iface blurConnection;
	private final String tableName;
	private final int tableId;
	private final TableDatabaseInterface database;

	public ServerCollector(Iface connection, String tableName, int tableId, TableDatabaseInterface database) {
		this.blurConnection = connection;
		this.tableName = tableName;
		this.tableId = tableId;
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
			this.database.updateTableServer(tableId, new ObjectMapper().writeValueAsString(serverLayout));

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