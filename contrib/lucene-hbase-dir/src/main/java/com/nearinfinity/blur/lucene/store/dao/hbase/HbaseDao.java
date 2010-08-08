package com.nearinfinity.blur.lucene.store.dao.hbase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.nearinfinity.blur.lucene.store.DirectoryStore;
import com.nearinfinity.blur.lucene.store.dao.DirectoryDao;

public class HbaseDao implements DirectoryStore {
	
	private static final byte SEP = '/';
	private HTable hTable;
	private byte[] columnFamily;
	private byte[] dirName;
	private boolean useCache = false;//not safe
	private int maxCacheSize = 16;

	public HbaseDao(String tableName, String columnFamily, String dirName) throws IOException {
		hTable = new HTable(tableName);
		this.columnFamily = Bytes.toBytes(columnFamily);
		this.dirName = Bytes.toBytes(dirName);
	}

	@Override
	public void close() throws IOException {
		hTable.close();
	}

	@Override
	public byte[] fetchBlock(String name, long blockId) throws IOException {
		byte[] qualifier = Bytes.toBytes(blockId);
		Result result = get(new Get(getRowId(Bytes.toBytes(name), blockId)).addColumn(columnFamily, qualifier));
		if (result.isEmpty()) {
			return null;
		}
		return result.getValue(columnFamily, qualifier);
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return !get(new Get(getRowId()).addColumn(columnFamily, Bytes.toBytes(name))).isEmpty();
	}

	@Override
	public List<String> getAllFileNames() throws IOException {
		List<String> list = new ArrayList<String>();
		Result result = get(new Get(getRowId()));
		if (result.isEmpty()) {
			return list;
		}
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
		for (Entry<byte[], byte[]> entry : familyMap.entrySet()) {
			if (Bytes.toLong(entry.getValue()) >= 0) {
				list.add(Bytes.toString(entry.getKey()));
			}
		}
		return list;
	}

	@Override
	public long getFileLength(String name) throws IOException {
		byte[] qualifier = Bytes.toBytes(name);
		Result result = get(new Get(getRowId()).addColumn(columnFamily, qualifier));
		if (result.isEmpty()) {
			throw new FileNotFoundException(name);
		}
		return Bytes.toLong(result.getValue(columnFamily, qualifier));
	}

	@Override
	public long getFileModified(String name) throws IOException {
		byte[] qualifier = Bytes.toBytes(name);
		Result result = get(new Get(getRowId()).addColumn(columnFamily, qualifier));
		if (result.isEmpty()) {
			throw new FileNotFoundException(name);
		}
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		NavigableMap<Long, byte[]> navigableMap = map.get(columnFamily).get(qualifier);
		Set<Long> keySet = navigableMap.keySet();
		return keySet.iterator().next();
	}

	@Override
	public void saveBlock(String name, long blockId, byte[] block) throws IOException {
		if (useCache) {
			List<Put> list = putCaches.get(name);
			if (list == null) {
				list = new ArrayList<Put>();
				putCaches.put(name, list);
			} else {
				if (list.size() >= maxCacheSize) {
					put(list);
					list.clear();
				}
			}
			list.add(new Put(getRowId(Bytes.toBytes(name),blockId)).add(columnFamily, Bytes.toBytes(blockId), block));
		} else {
			put(new Put(getRowId(Bytes.toBytes(name),blockId)).add(columnFamily, Bytes.toBytes(blockId), block));
		}
	}
	
	@Override
	public void removeBlock(String name, long blockId) {
		delete(new Delete(getRowId(Bytes.toBytes(name),blockId)));
	}


	@Override
	public void setFileLength(String name, long length) throws IOException {
		put(new Put(getRowId()).add(columnFamily, Bytes.toBytes(name), Bytes.toBytes(length)));
	}
	
	private byte[] getRowId(byte[] fileName, long blockId) {
		return ByteBuffer.allocate(dirName.length + fileName.length + 8 + 2).
			put(dirName).put(SEP).
			put(fileName).put(SEP).
			putLong(blockId).
			array();
	}
	
	private byte[] getRowId() {
		return dirName;
	}
	
	private Result get(Get get) {
		try {
			synchronized (hTable) {
				return hTable.get(get);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void put(Put put) {
		try {
			synchronized (hTable) {
				hTable.put(put);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void delete(Delete delete) {
		try {
			synchronized (hTable) {
				hTable.delete(delete);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void put(List<Put> puts) {
		try {
			synchronized (hTable) {
				hTable.put(puts);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private Map<String,List<Put>> putCaches = new HashMap<String, List<Put>>();

	@Override
	public void flush(String name) throws IOException {
		if (useCache) {
			List<Put> list = putCaches.remove(name);
			if (list != null) {
				put(list);
			}
		}
	}

}
