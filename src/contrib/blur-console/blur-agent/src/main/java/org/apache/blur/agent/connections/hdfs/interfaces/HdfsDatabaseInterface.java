package org.apache.blur.agent.connections.hdfs.interfaces;

import java.util.Date;

import org.apache.blur.agent.exceptions.NullReturnedException;


public interface HdfsDatabaseInterface {
	void setHdfsInfo(String name, String host, int port);

	int getHdfsId(String name) throws NullReturnedException;

	void insertHdfsStats(long capacity, long presentCapacity, long remaining, long used, long logical_used, double d,
			long underReplicatedBlocksCount, long corruptBlocksCount, long missingBlocksCount, long totalNodes, long liveNodes, long deadNodes,
			Date time, String host, int port, int hdfsId);
}
