package com.nearinfinity.blur.store;

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
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

public class CassandraDirectory extends Directory {

	public static final long BLOCK_SHIFT = 14; //2^19 = 524,288 bits which is 65,536 bytes per block
	public static final long BLOCK_MOD = 0x3FFF;
	public static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
	private static final String SEP = "/";
	
	public static long getBlock(long pos) {
		return pos >>> BLOCK_SHIFT;
	}
	
	public static long getPosition(long pos) {
		return pos & BLOCK_MOD;
	}
	
	public static long getRealPosition(long block, long positionInBlock) {
		return (block << BLOCK_SHIFT) + positionInBlock;
	}
	
	private ClientPool pool;
	private ConsistencyLevel consistencyLevel;
	private String keySpace;
	private String columnFamily;
	private String dirName;
	
	public CassandraDirectory(String keySpace, String columnFamily, String dirName, ConsistencyLevel consistencyLevel, int poolSize, String hostName, int port) throws Exception {
		this.keySpace = keySpace;
		this.columnFamily = columnFamily;
		this.dirName = dirName;
		this.consistencyLevel = consistencyLevel;
		this.pool = new ClientPool(poolSize, hostName, port);
		setLockFactory(new NoLockFactory());
	}
	
	@Override
	public void close() throws IOException {
		pool.close();
	}

	@Override
	public void deleteFile(String name) throws IOException {
		setFileLength(name, -1);
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
	public long fileLength(String name) throws IOException {
		long fileLength = getFileLength(name);
		if (fileLength < 0) {
			throw new FileNotFoundException(name + " return [" + fileLength + "]");
		}
		return fileLength;
	}

	@Override
	public long fileModified(String name) throws IOException {
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

	@Override
	public String[] listAll() throws IOException {
		Client client = pool.getClient();
		try {
			SlicePredicate slicePredicate = new SlicePredicate();
			SliceRange sliceRange = new SliceRange(Bytes.EMPTY_BYTES, Bytes.EMPTY_BYTES, false, Integer.MAX_VALUE);
			slicePredicate.setSlice_range(sliceRange);
			ColumnParent columnParent = new ColumnParent(columnFamily);
			List<ColumnOrSuperColumn> list = client.get_slice(keySpace, getDirectoryId(), columnParent, slicePredicate, consistencyLevel);
			List<String> result = new ArrayList<String>();
			for (ColumnOrSuperColumn column : list) {
				if (Bytes.toLong(column.column.value) >= 0) {
					result.add(Bytes.toString(column.column.name));
				}
			}
			return result.toArray(new String[]{});
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}

	@Override
	public void touchFile(String name) throws IOException {
		long fileLength = getFileLength(name);
		setFileLength(name, fileLength < 0 ? 0 : fileLength);
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		setFileLength(name, 0);
		return new BufferedIndexOutput() {
			
			private long position;
			private long fileLength = 0;

			@Override
			public long length() throws IOException {
				return fileLength;
			}
			
			@Override
			protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
				while (len > 0) {
					long blockId = getBlock(position);
					int innerPosition = (int) getPosition(position);
					byte[] block = fetchBlock(name,blockId);
					if (block == null) {
						block = new byte[BLOCK_SIZE];
					}
					int length = Math.min(len, block.length - innerPosition);
					System.arraycopy(b, offset, block, innerPosition, length);
					saveBlock(name,blockId,block);
					position += length;
					len -= length;
					offset += length;
				}
				if (position > fileLength) {
					setLength(position);
				}
			}

			@Override
			public void close() throws IOException {
				super.close();
			}

			@Override
			public void seek(long pos) throws IOException {
				super.seek(pos);
				this.position = pos;
			}

			@Override
			public void setLength(final long length) throws IOException {
				super.setLength(length);
				fileLength = length;
				setFileLength(name,length);
			}
		};
	}


	@Override
	public IndexInput openInput(final String name) throws IOException {
		if (!fileExists(name)) {
			touchFile(name);
		}
		final long fileLength = fileLength(name);
		return new BufferedIndexInput(BLOCK_SIZE) {
			@Override
			public long length() {
				return fileLength;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
			@Override
			protected void seekInternal(long pos) throws IOException {
			}
			
			@Override
			protected void readInternal(byte[] b, int off, int len) throws IOException {
				long position = getFilePointer();
				while (len > 0) {
					long blockId = getBlock(position);
					int innerPosition = (int) getPosition(position);
					byte[] block = fetchBlock(name, blockId);
					int length = Math.min(len,block.length-innerPosition);
					System.arraycopy(block, innerPosition, b, off, length);
					position += length;
					len -= length;
					off += length;
				}
			}
		};
	}
	
	private void saveBlock(String name, long blockId, byte[] block) {
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

	private byte[] fetchBlock(String name, long blockId) throws IOException {
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
	
	private void setFileLength(String name, long length) {
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
	
	private long getFileLength(String name) throws FileNotFoundException {
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

	private String getDirectoryId() {
		return dirName;
	}
	
	private String getDirectoryId(String name) {
		return dirName + SEP + name;
	}

}
