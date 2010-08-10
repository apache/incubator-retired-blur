package com.nearinfinity.blur.lucene.store.dao.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.Cassandra.Client;

import com.nearinfinity.blur.lucene.store.DirectoryStore;
import com.nearinfinity.blur.lucene.store.dao.cassandra.CassandraExecutor.Command;
import com.nearinfinity.blur.lucene.store.dao.cassandra.utils.Bytes;

public class CassandraStore implements DirectoryStore {
	
	private static final String SEP = "/";
	private ConsistencyLevel consistencyLevel;
	private String keySpace;
	private String columnFamily;
	private String dirName;
	
	public CassandraStore(String keySpace, String columnFamily, String dirName, ConsistencyLevel consistencyLevel, int poolSize, String hostName, int port) throws Exception {
		this.keySpace = keySpace;
		this.columnFamily = columnFamily;
		this.dirName = dirName;
		this.consistencyLevel = consistencyLevel;
		CassandraExecutor.setup(port, poolSize, hostName);
	}
	
	@Override
	public void removeFileMetaData(final String name) throws IOException {
		CassandraExecutor.execute(new Command<Boolean>() {
			@Override
			public Boolean execute(Client client) throws Exception {
				ColumnPath columnPath = new ColumnPath(columnFamily);
				columnPath.setColumn(Bytes.toBytes(name));
				client.remove(keySpace, getDirectoryId(), columnPath, System.currentTimeMillis(), consistencyLevel);
				return true;
			}
		});
	}
	
	@Override
	public long getFileModified(final String name) throws IOException {
		return CassandraExecutor.execute(new Command<Long>() {
			@Override
			public Long execute(Client client) throws Exception {
				ColumnPath columnPath = new ColumnPath(columnFamily);
				columnPath.setColumn(Bytes.toBytes(name));
				ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(), columnPath, consistencyLevel);
				return column.column.timestamp;
			}
		});
	}

	@Override
	public long getFileLength(final String name) throws IOException {
		return CassandraExecutor.execute(new Command<Long>() {
			@Override
			public Long execute(Client client) throws Exception {
				try {
					ColumnPath columnPath = new ColumnPath(columnFamily);
					columnPath.setColumn(Bytes.toBytes(name));
					ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(), columnPath, consistencyLevel);
					return Bytes.toLong(column.column.value);
				} catch (NotFoundException e) {
					return -1l;
				}
			}
		});
	}
	
	public void setFileLength(final String name, final long length) throws IOException {
		CassandraExecutor.execute(new Command<Boolean>() {
			@Override
			public Boolean execute(Client client) throws Exception {
				ColumnPath columnPath = new ColumnPath(columnFamily);
				columnPath.setColumn(Bytes.toBytes(name));
				client.insert(keySpace, getDirectoryId(), columnPath, Bytes.toBytes(length), System.currentTimeMillis(), consistencyLevel);
				return true;
			}
		});
	}

	@Override
	public void close() {
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		long fileLength = getFileLength(name);
		if (fileLength < 0) {
			return false;
		}
		return true;
	}

	@Override
	public List<String> getAllFileNames() throws IOException {
		return CassandraExecutor.execute(new Command<List<String>>() {
			@Override
			public List<String> execute(Client client) throws Exception {
				SlicePredicate slicePredicate = new SlicePredicate();
				SliceRange sliceRange = new SliceRange(Bytes.EMPTY_BYTE_ARRAY, Bytes.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
				slicePredicate.setSlice_range(sliceRange);
				ColumnParent columnParent = new ColumnParent(columnFamily);
				List<ColumnOrSuperColumn> list = client.get_slice(keySpace, getDirectoryId(), columnParent, slicePredicate, consistencyLevel);
				List<String> result = new ArrayList<String>();
				for (ColumnOrSuperColumn column : list) {
					if (Bytes.toLong(column.column.value) >= 0) {
						result.add(Bytes.toString(column.column.name));
					}
				}
				return result;
			}
		});
	}

	@Override
	public void flush(String name) throws IOException {
		
	}

	@Override
	public void removeBlock(final String name, final long blockId) throws IOException {
		CassandraExecutor.execute(new Command<Boolean>() {
			@Override
			public Boolean execute(Client client) throws Exception {
				ColumnPath columnPath = new ColumnPath(columnFamily);
				columnPath.setColumn(Bytes.toBytes(blockId));
				client.remove(keySpace, getDirectoryId(name), columnPath, System.currentTimeMillis(), consistencyLevel);
				return true;
			}
		});
	}
	
	public void saveBlock(final String name, final long blockId, final byte[] block) throws IOException {
		CassandraExecutor.execute(new Command<Boolean>() {
			@Override
			public Boolean execute(Client client) throws Exception {
				ColumnPath columnPath = new ColumnPath(columnFamily);
				columnPath.setColumn(Bytes.toBytes(blockId));
				client.insert(keySpace, getDirectoryId(name), columnPath, block, System.currentTimeMillis(), consistencyLevel);
				return true;
			}
		});
	}

	public byte[] fetchBlock(final String name, final long blockId) throws IOException {
		return CassandraExecutor.execute(new Command<byte[]>() {
			@Override
			public byte[] execute(Client client) throws Exception {
				try {
					ColumnPath columnPath = new ColumnPath(columnFamily);
					columnPath.setColumn(Bytes.toBytes(blockId));
					ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(name), columnPath, consistencyLevel);
					return column.column.value;
				} catch (NotFoundException e) {
					return null;
				}
			}
		});
	}
	
	private String getDirectoryId() {
		return dirName;
	}
	
	private String getDirectoryId(String name) {
		return dirName + SEP + name;
	}

}
