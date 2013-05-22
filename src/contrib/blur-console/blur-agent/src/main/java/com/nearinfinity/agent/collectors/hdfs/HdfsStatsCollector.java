package com.nearinfinity.agent.collectors.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.springframework.dao.DataAccessException;

import com.nearinfinity.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import com.nearinfinity.agent.exceptions.NullReturnedException;
import com.nearinfinity.agent.types.TimeHelper;

public class HdfsStatsCollector implements Runnable {
	private final static Log log = LogFactory.getLog(HdfsStatsCollector.class);

	private final String hdfsName;
	private final URI uri;
	private final String host;
	private final int port;
	private final String user;
	private final HdfsDatabaseInterface database;

	public HdfsStatsCollector(final String hdfsName, final URI uri, final String user, final HdfsDatabaseInterface database) {
		this.uri = uri;
		this.host = uri.getHost();
		this.port = uri.getPort();
		this.hdfsName = hdfsName;
		this.user = user;
		this.database = database;
	}

	@Override
	public void run() {
		try {
			int hdfsId = this.database.getHdfsId(this.hdfsName);
			if (hdfsId == -1) {
				log.error("The HDFS [" + this.hdfsName + "] does not exist in the database");
				return;
			}

			// Creates a filesystem connection (if a user is given
			// then the filesystem can get additional information)
			FileSystem fileSystem = (this.user != null) ? FileSystem.get(this.uri, new Configuration(), this.user) : FileSystem.get(this.uri,
					new Configuration());

			if (fileSystem instanceof DistributedFileSystem) {
				DistributedFileSystem dfs = (DistributedFileSystem) fileSystem;

				// TODO: Need to figure out how to do hadoop version check for this information.
				//FsStatus ds = dfs.getStatus();
				long capacity = -1;//ds.getCapacity();
				long used = -1;//ds.getUsed();
				long logical_used = used / dfs.getDefaultReplication();
				long remaining = -1; //ds.getRemaining();
				long presentCapacity = used + remaining;

				long liveNodes = -1;
				long deadNodes = -1;
				long totalNodes = -1;

				try {
					DatanodeInfo[] live = dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
					DatanodeInfo[] dead = dfs.getClient().datanodeReport(DatanodeReportType.DEAD);

					liveNodes = live.length;
					deadNodes = dead.length;
					totalNodes = liveNodes + deadNodes;
				} catch (Exception e) {
					log.warn("Access denied for user. Skipping node information.");
				}

				this.database.insertHdfsStats(capacity, presentCapacity, remaining, used, logical_used, (((1.0 * used) / presentCapacity) * 100),
						dfs.getUnderReplicatedBlocksCount(), dfs.getCorruptBlocksCount(), dfs.getMissingBlocksCount(), totalNodes, liveNodes,
						deadNodes, TimeHelper.now().getTime(), host, port, hdfsId);

				dfs.close();
			}
		} catch (IOException e) {
			log.error("An IO error occurred while communicating with the DFS.", e);
		} catch (DataAccessException e) {
			log.error("An error occurred while writing the HDFSStats to the DB.", e);
		} catch (NullReturnedException e) {
			log.error(e.getMessage(), e);
		} catch (Exception e) {
			log.error("An unknown error occurred in the CollectorManager.", e);
		}
	}
}
