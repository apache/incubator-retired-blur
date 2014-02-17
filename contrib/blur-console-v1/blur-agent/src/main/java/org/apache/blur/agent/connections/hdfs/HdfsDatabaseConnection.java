package org.apache.blur.agent.connections.hdfs;

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
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.blur.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import org.apache.blur.agent.exceptions.NullReturnedException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;


public class HdfsDatabaseConnection implements HdfsDatabaseInterface {
	private final JdbcTemplate jdbc;

	public HdfsDatabaseConnection(JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	@Override
	public void setHdfsInfo(String name, String host, int port) {
		List<Map<String, Object>> existingHdfs = jdbc.queryForList("select id from hdfs where name=?", name);

		if (existingHdfs.isEmpty()) {
			jdbc.update("insert into hdfs (name, host, port) values (?, ?, ?)", name, host, port);
		} else {
			jdbc.update("update hdfs set host=?, port=? where id=?", host, port, existingHdfs.get(0).get("ID"));
		}
	}

	@Override
	public int getHdfsId(String name) throws NullReturnedException {
		try {
			return jdbc.queryForInt("select id from hdfs where name = ?", name);
		} catch (IncorrectResultSizeDataAccessException e) {
			return -1;
		}
	}

	@Override
	public void insertHdfsStats(long capacity, long presentCapacity, long remaining, long used, long logical_used, double d,
			long underReplicatedBlocksCount, long corruptBlocksCount, long missingBlocksCount, long totalNodes, long liveNodes, long deadNodes,
			Date time, String host, int port, int hdfsId) {
		jdbc.update(
				"insert into hdfs_stats (config_capacity, present_capacity, dfs_remaining, dfs_used_real, dfs_used_logical, dfs_used_percent, under_replicated, corrupt_blocks, missing_blocks, total_nodes, live_nodes, dead_nodes, created_at, host, port, hdfs_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
				capacity, presentCapacity, remaining, used, logical_used, (((1.0 * used) / presentCapacity) * 100), underReplicatedBlocksCount,
				corruptBlocksCount, missingBlocksCount, totalNodes, liveNodes, deadNodes, time, host, port, hdfsId);
	}
}
