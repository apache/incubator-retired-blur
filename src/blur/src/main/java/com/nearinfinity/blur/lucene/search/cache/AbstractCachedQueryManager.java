package com.nearinfinity.blur.lucene.search.cache;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;

import com.nearinfinity.blur.utils.bitset.compression.CompressedBitSet;
import com.nearinfinity.blur.utils.bitset.compression.CompressedBitSetNoCompression;

public abstract class AbstractCachedQueryManager<T> {
	
	public static final CompressedBitSet EMPTY = new CompressedBitSetNoCompression(new OpenBitSet());
	
	private static final int _10000 = 10000;
	private final static Log LOG = LogFactory.getLog(AbstractCachedQueryManager.class);
	private static final String CACHED_QUERY_MANAGER = "CachedQueryManager-";
	
	private Thread cachedQueryManagerThread;
	
	private double compressionThreshold = 0.5;
	protected Map<String,Query> cachedQueries = new ConcurrentHashMap<String, Query>();
	protected Map<IndexReader,Map<String,CompressedBitSet>> cachedBitSets = Collections.synchronizedMap(new WeakHashMap<IndexReader, Map<String,CompressedBitSet>>());
	
	public AbstractCachedQueryManager(String name, boolean auto) {
		if (auto) {
			cachedQueryManagerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							Collection<String> names = new TreeSet<String>(cachedQueries.keySet());
							updateFacets(names);
						} catch (Exception e) {
							LOG.error("unknown error",e);
						}
						try {
							Thread.sleep(_10000);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					}
				}
			});
			cachedQueryManagerThread.setName(CACHED_QUERY_MANAGER + name);
			cachedQueryManagerThread.setDaemon(true);
			cachedQueryManagerThread.setPriority(Thread.MIN_PRIORITY);
			cachedQueryManagerThread.start();
		}
	}
	
	public abstract T create(String... names);

	public void add(String name, Query query) {
		cachedQueries.put(name, query);
	}
	
	public Query remove(String name) {
		return cachedQueries.remove(name);
	}
	
	public Query get(String name) {
		return cachedQueries.get(name);
	}
	
	public void updateFacets(Collection<String> names) throws IOException {
		for (String name : names) {
			Query query = cachedQueries.get(name);
			if (query != null) {
				updateFacet(name,query);
			}
		}
	}

	private void updateFacet(String name, Query query) throws IOException {
		Collection<IndexReader> readers = new HashSet<IndexReader>(cachedBitSets.keySet());
		for (IndexReader reader : readers) {
			Map<String, CompressedBitSet> map = cachedBitSets.get(reader);
			if (!map.containsKey(name)) {
				CompressedBitSet bitSet = getCompressedBitSet(reader,query);
				map.put(name, bitSet);
			}
		}
	}

	private CompressedBitSet getCompressedBitSet(IndexReader reader, Query query) throws IOException {
		IndexSearcher searcher = new IndexSearcher(reader);
		final OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
		searcher.search(query, new Collector() {
			
			@Override
			public void setScorer(Scorer scorer) throws IOException {
				
			}
			
			@Override
			public void setNextReader(IndexReader reader, int docBase) throws IOException {
				
			}
			
			@Override
			public void collect(int doc) throws IOException {
				bitSet.set(doc);
			}
			
			@Override
			public boolean acceptsDocsOutOfOrder() {
				return false;
			}
		});
		return CompressedBitSet.create(bitSet, compressionThreshold);
	}
	
	public void update(IndexReader reader) {
		IndexReader[] readers = reader.getSequentialSubReaders();
		if (readers == null) {
			updateReader(reader);
		}
		for (IndexReader indexReader : readers) {
			updateReader(indexReader);
		}
	}

	private void updateReader(IndexReader reader) {
		if (!cachedBitSets.containsKey(reader)) {
			cachedBitSets.put(reader, Collections.synchronizedMap(new WeakHashMap<String, CompressedBitSet>()));
		}
	}

	public void updateAllFacets() throws IOException {
		updateFacets(new HashSet<String>(cachedQueries.keySet()));
	}
	
	protected Map<IndexReader, CompressedBitSet[]> getCompressedBitSetsByName(String[] names) {
		Map<IndexReader, CompressedBitSet[]> result = new HashMap<IndexReader, CompressedBitSet[]>();
		for (IndexReader indexReader : cachedBitSets.keySet()) {
			CompressedBitSet[] bitSets = new CompressedBitSet[names.length];
			Map<String, CompressedBitSet> map = cachedBitSets.get(indexReader);
			for (int i = 0; i < names.length; i++) {
				CompressedBitSet compressedBitSet = map.get(names[i]);
				if (compressedBitSet == null) {
					bitSets[i] = EMPTY;
				} else {
					bitSets[i] = compressedBitSet;
				}
			}
			result.put(indexReader, bitSets);
		}
		return result;
	}
}
