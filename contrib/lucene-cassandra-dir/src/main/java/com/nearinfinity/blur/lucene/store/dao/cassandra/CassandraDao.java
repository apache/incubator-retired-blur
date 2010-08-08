package com.nearinfinity.blur.lucene.store.dao.cassandra;

import java.io.FileNotFoundException;
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

public class CassandraDao implements DirectoryStore {
	
	private static final String SEP = "/";
	private ClientPool pool;
	private ConsistencyLevel consistencyLevel;
	private String keySpace;
	private String columnFamily;
	private String dirName;
	
	public CassandraDao(String keySpace, String columnFamily, String dirName, ConsistencyLevel consistencyLevel, int poolSize, String hostName, int port) throws Exception {
		this.keySpace = keySpace;
		this.columnFamily = columnFamily;
		this.dirName = dirName;
		this.consistencyLevel = consistencyLevel;
		this.pool = new ClientPool(poolSize, hostName, port);
	}
	
	@Override
	public long getFileModified(String name) throws IOException {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setColumn(Bytes.toBytes(name));
			ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(), columnPath, consistencyLevel);
			return column.column.timestamp;
		} catch (NotFoundException e) {
			throw new FileNotFoundException(name);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}
	
	private String getDirectoryId() {
		return dirName;
	}
	
	private String getDirectoryId(String name) {
		return dirName + SEP + name;
	}

	@Override
	public long getFileLength(String name) {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setColumn(Bytes.toBytes(name));
			ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(), columnPath, consistencyLevel);
			return Bytes.toLong(column.column.value);
		} catch (NotFoundException e) {
			return -2;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}
	
	public void saveBlock(String name, long blockId, byte[] block) {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setColumn(Bytes.toBytes(blockId));
			client.insert(keySpace, getDirectoryId(name), columnPath, block, System.currentTimeMillis(), consistencyLevel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}

	public byte[] fetchBlock(String name, long blockId) throws IOException {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setColumn(Bytes.toBytes(blockId));
			ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(name), columnPath, consistencyLevel);
			return column.column.value;
		} catch (NotFoundException e) {
			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}
	
	public void setFileLength(String name, long length) {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setColumn(Bytes.toBytes(name));
			client.insert(keySpace, getDirectoryId(), columnPath, Bytes.toBytes(length), System.currentTimeMillis(), consistencyLevel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}

	@Override
	public void close() {
		pool.close();
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
		Client client = pool.getClient();
		try {
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
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}

	@Override
	public void flush(String name) throws IOException {
		
	}

	@Override
	public void removeBlock(String name, long blockId) {
		throw new RuntimeException("not implemented");
	}
	
}
