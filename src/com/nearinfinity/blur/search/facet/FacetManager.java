package com.nearinfinity.blur.search.facet;

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

import com.nearinfinity.lucene.util.CompressedBitSet;
import com.nearinfinity.lucene.util.CompressedBitSetNoCompression;

public class FacetManager {
	
	public static final CompressedBitSet EMPTY = new CompressedBitSetNoCompression(new OpenBitSet());

	private static final int _10000 = 10000;
	private final static Log LOG = LogFactory.getLog(FacetManager.class);
	private static final String FACET_MANAGER = "FacetManager-";
	private double compressionThreshold = 0.5;
	private Map<String,Query> facetQueries = new ConcurrentHashMap<String, Query>();
	private Map<IndexReader,Map<String,CompressedBitSet>> facetBitSets = Collections.synchronizedMap(new WeakHashMap<IndexReader, Map<String,CompressedBitSet>>());
	private Thread facetManagerThread;
	
	public FacetManager(String name, boolean auto) {
		if (auto) {
			facetManagerThread = new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						try {
							Collection<String> facetNames = new TreeSet<String>(facetQueries.keySet());
							updateFacets(facetNames);
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
			facetManagerThread.setName(FACET_MANAGER + name);
			facetManagerThread.setDaemon(true);
			facetManagerThread.setPriority(Thread.MIN_PRIORITY);
			facetManagerThread.start();
		}
	}
	
	public void updateFacets(Collection<String> names) throws IOException {
		for (String name : names) {
			Query query = facetQueries.get(name);
			if (query != null) {
				updateFacet(name,query);
			}
		}
	}

	private void updateFacet(String name, Query query) throws IOException {
		Collection<IndexReader> readers = new HashSet<IndexReader>(facetBitSets.keySet());
		for (IndexReader reader : readers) {
			Map<String, CompressedBitSet> map = facetBitSets.get(reader);
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

	public Map<IndexReader, CompressedBitSet[]> getFacets(String[] names) {
		Map<IndexReader, CompressedBitSet[]> result = new HashMap<IndexReader, CompressedBitSet[]>();
		for (IndexReader indexReader : facetBitSets.keySet()) {
			CompressedBitSet[] bitSets = new CompressedBitSet[names.length];
			Map<String, CompressedBitSet> map = facetBitSets.get(indexReader);
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

	public void add(String name, Query query) {
		facetQueries.put(name, query);
	}
	
	public Query remove(String name) {
		return facetQueries.remove(name);
	}
	
	public Query get(String name) {
		return facetQueries.get(name);
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
		if (!facetBitSets.containsKey(reader)) {
			facetBitSets.put(reader, Collections.synchronizedMap(new WeakHashMap<String, CompressedBitSet>()));
		}
	}

	public void updateAllFacets() throws IOException {
		updateFacets(new HashSet<String>(facetQueries.keySet()));
	}
}
