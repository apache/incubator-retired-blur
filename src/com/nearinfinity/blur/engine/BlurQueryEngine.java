package com.nearinfinity.blur.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.lucene.document.Document;

import com.nearinfinity.blur.BlurHit;
import com.nearinfinity.blur.BlurQuery;
import com.nearinfinity.blur.index.SuperDocument;
import com.nearinfinity.blur.store.Bytes;
import com.nearinfinity.blur.store.ClientPool;

public class BlurQueryEngine implements BlurQuery {
	
	private static final byte[] EMPTY = new byte[]{};
	private ClientPool pool;
	private String keySpace;
	private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
	private Collection<String> columnFamilies;
	
	public BlurQueryEngine() {
		//setup clientpool
		//set keyspace
		//read column families
	}

	@Override
	public Document getDocument(String id, String columnFamily, String superKey) {
		Client client = pool.getClient();
		try {
			ColumnPath columnPath = new ColumnPath(columnFamily);
			columnPath.setSuper_column(Bytes.toBytes(superKey));
			ColumnOrSuperColumn columnOrSuperColumn = client.get(keySpace, id, columnPath, consistencyLevel);
			SuperColumn superColumn = columnOrSuperColumn.super_column;
			return getDoc(superColumn);
		} catch (NotFoundException e) {
			return null;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}

	@Override
	public SuperDocument getSuperDocument(String id) {
		SuperDocument document = new SuperDocument(id);
		for (String columnFamily : columnFamilies) {
			Map<String, Document> superDocumentSlice = getSuperDocumentSlice(id, columnFamily);
			if (!superDocumentSlice.isEmpty()) {
				document.setDocuments(columnFamily, superDocumentSlice);
			}
		}
		if (document.isEmpty()) {
			return null;
		}
		return document;
	}

	@Override
	public Map<String, Document> getSuperDocumentSlice(String id, String columnFamily) {
		Client client = pool.getClient();
		try {
			SlicePredicate slicePredicate = new SlicePredicate();
			SliceRange sliceRange = new SliceRange(EMPTY, EMPTY, false, Integer.MAX_VALUE);
			slicePredicate.setSlice_range(sliceRange);
			List<ColumnOrSuperColumn> slice = client.get_slice(keySpace, id, new ColumnParent(columnFamily), slicePredicate, consistencyLevel);
			Map<String,Document> docs = new TreeMap<String, Document>();
			for (ColumnOrSuperColumn column : slice) {
				SuperColumn superColumn = column.super_column;
				docs.put(Bytes.toString(superColumn.name), getDoc(superColumn));
			}
			return docs;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			pool.putClient(client);
		}
	}
	
	private Document getDoc(SuperColumn superColumn) {
		return null;
	}
	
	
	
	

	@Override
	public long searchFast(String query, String filter) {
		return searchFast(query, filter, Long.MAX_VALUE);
	}

	@Override
	public long searchFast(String query, String filter, long minimum) {
		return 0;
	}

	@Override
	public List<BlurHit> searchFast(String query, String filter, int starting, int fetch) {
		return null;
	}
	


}
