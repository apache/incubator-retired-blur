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
import java.util.List;
import java.util.Map;

import org.apache.blur.agent.connections.blur.interfaces.TableDatabaseInterface;
import org.apache.blur.agent.exceptions.NullReturnedException;
import org.apache.blur.agent.types.Column;
import org.apache.blur.agent.types.Family;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.dao.DataAccessException;


public class SchemaCollector implements Runnable {
	private static final Log log = LogFactory.getLog(SchemaCollector.class);

	private final Iface blurConnection;
	private final String tableName;
	private final int tableId;
	private final TableDescriptor descriptor;
	private final TableDatabaseInterface database;

	public SchemaCollector(Iface connection, String tableName, int tableId, TableDescriptor descriptor, TableDatabaseInterface database) {
		this.blurConnection = connection;
		this.tableName = tableName;
		this.tableId = tableId;
		this.descriptor = descriptor;
		this.database = database;
	}

	@Override
	public void run() {
		try {
			Schema schema = null;
			schema = blurConnection.schema(tableName);
			if (schema == null || descriptor == null) {
				throw new NullReturnedException("No Schema or Descriptor Defined!");
			}

			List<Family> columnDefs = getColumnDefinitions(schema);

			this.database.updateTableSchema(this.tableId, new ObjectMapper().writeValueAsString(columnDefs), "UNKNOWN");
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
		Map<String, Map<String, ColumnDefinition>> columnFamilies = schema.getFamilies();
		if (columnFamilies != null) {
			for (Map.Entry<String, Map<String, ColumnDefinition>> schemaEntry : columnFamilies.entrySet()) {
				Family family = new Family(schemaEntry.getKey());
				for (ColumnDefinition def : schemaEntry.getValue().values()) {
					Column column = new Column(def.getColumnName());
					column.setFullText(def.isFieldLessIndexed());
					//TODO: Is the analyzer available anymore?
					//TODO: Add field type
					//TODO: Do anything with subcolumns?
					column.setLive(true);
					family.getColumns().add(column);
				}
				columnDefs.add(family);
			}
		}
		return columnDefs;
	}
}
