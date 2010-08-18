package com.nearinfinity.blur.lucene.store.dao.cassandra.storageproxy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;

import com.nearinfinity.blur.lucene.store.DirectoryStore;
import com.nearinfinity.blur.lucene.store.dao.cassandra.thrift.CassandraExecutor.Command;
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
	}
	
	@Override
	public void removeFileMetaData(final String name) throws IOException {
//		CassandraExecutor.execute(new Command<Boolean>() {
//			@Override
//			public Boolean execute(Client client) throws Exception {
//				ColumnPath columnPath = new ColumnPath(columnFamily);
//				columnPath.setColumn(Bytes.toBytes(name));
//				client.remove(keySpace, getDirectoryId(), columnPath, System.currentTimeMillis(), consistencyLevel);
//				return true;
//			}
//		});
	}
	
	@Override
	public long getFileModified(final String name) throws IOException {
		try {
			byte[] columnName = Bytes.toBytes(name);
			List<Row> rows = StorageProxy.readProtocol(getReadCommand(getDirectoryId(),columnName), consistencyLevel);
			if (rows.isEmpty()) {
				throw new FileNotFoundException(name);
			}
			Row row = rows.get(0);
			IColumn column = row.cf.getColumn(columnName);
			if (column.isMarkedForDelete()) {
				throw new FileNotFoundException(name);
			}
			return column.timestamp();
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		}
	}



	@Override
	public long getFileLength(final String name) throws IOException {
		try {
			byte[] columnName = Bytes.toBytes(name);
			List<Row> rows = StorageProxy.readProtocol(getReadCommand(getDirectoryId(),columnName), consistencyLevel);
			if (rows.isEmpty()) {
				throw new FileNotFoundException(name);
			}
			Row row = rows.get(0);
			IColumn column = row.cf.getColumn(columnName);
			if (column.isMarkedForDelete()) {
				throw new FileNotFoundException(name);
			}
			return Bytes.toLong(column.value());
		} catch (UnavailableException e) {
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			throw new RuntimeException(e);
		} catch (InvalidRequestException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void setFileLength(final String name, final long length) throws IOException {
//		CassandraExecutor.execute(new Command<Boolean>() {
//			@Override
//			public Boolean execute(Client client) throws Exception {
//				ColumnPath columnPath = new ColumnPath(columnFamily);
//				columnPath.setColumn(Bytes.toBytes(name));
//				client.insert(keySpace, getDirectoryId(), columnPath, Bytes.toBytes(length), System.currentTimeMillis(), consistencyLevel);
//				return true;
//			}
//		});
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
//		return CassandraExecutor.execute(new Command<List<String>>() {
//			@Override
//			public List<String> execute(Client client) throws Exception {
//				SlicePredicate slicePredicate = new SlicePredicate();
//				SliceRange sliceRange = new SliceRange(Bytes.EMPTY_BYTE_ARRAY, Bytes.EMPTY_BYTE_ARRAY, false, Integer.MAX_VALUE);
//				slicePredicate.setSlice_range(sliceRange);
//				ColumnParent columnParent = new ColumnParent(columnFamily);
//				List<ColumnOrSuperColumn> list = client.get_slice(keySpace, getDirectoryId(), columnParent, slicePredicate, consistencyLevel);
//				List<String> result = new ArrayList<String>();
//				for (ColumnOrSuperColumn column : list) {
//					if (Bytes.toLong(column.column.value) >= 0) {
//						result.add(Bytes.toString(column.column.name));
//					}
//				}
//				return result;
//			}
//		});
		return null;
	}

	@Override
	public void flush(String name) throws IOException {
		
	}

	@Override
	public void removeBlock(final String name, final long blockId) throws IOException {
//		CassandraExecutor.execute(new Command<Boolean>() {
//			@Override
//			public Boolean execute(Client client) throws Exception {
//				ColumnPath columnPath = new ColumnPath(columnFamily);
//				columnPath.setColumn(Bytes.toBytes(blockId));
//				client.remove(keySpace, getDirectoryId(name), columnPath, System.currentTimeMillis(), consistencyLevel);
//				return true;
//			}
//		});
	}
	
	public void saveBlock(final String name, final long blockId, final byte[] block) throws IOException {
//		CassandraExecutor.execute(new Command<Boolean>() {
//			@Override
//			public Boolean execute(Client client) throws Exception {
//				ColumnPath columnPath = new ColumnPath(columnFamily);
//				columnPath.setColumn(Bytes.toBytes(blockId));
//				client.insert(keySpace, getDirectoryId(name), columnPath, block, System.currentTimeMillis(), consistencyLevel);
//				return true;
//			}
//		});
	}

	public byte[] fetchBlock(final String name, final long blockId) throws IOException {
//		return CassandraExecutor.execute(new Command<byte[]>() {
//			@Override
//			public byte[] execute(Client client) throws Exception {
//				try {
//					ColumnPath columnPath = new ColumnPath(columnFamily);
//					columnPath.setColumn(Bytes.toBytes(blockId));
//					ColumnOrSuperColumn column = client.get(keySpace, getDirectoryId(name), columnPath, consistencyLevel);
//					return column.column.value;
//				} catch (NotFoundException e) {
//					return null;
//				}
//			}
//		});
		return null;
	}
	
	private String getDirectoryId() {
		return dirName;
	}
	
	private String getDirectoryId(String name) {
		return dirName + SEP + name;
	}

	private List<ReadCommand> getReadCommand(String id, byte[] columnName) {
		SliceByNamesReadCommand read = new SliceByNamesReadCommand(keySpace, getDirectoryId(), new ColumnParent(columnFamily), Arrays.asList(columnName));
		List<ReadCommand> commands = new ArrayList<ReadCommand>();
		commands.add(read);
		return commands;
	}
}
