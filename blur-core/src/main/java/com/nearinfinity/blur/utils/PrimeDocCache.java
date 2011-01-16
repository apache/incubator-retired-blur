package com.nearinfinity.blur.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;

import com.nearinfinity.blur.utils.bitset.BlurBitSet;

public class PrimeDocCache implements BlurConstants {
    
    private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);

    public static final BlurBitSet EMPTY_BIT_SET = new BlurBitSet();
    private static final Term PRIME_DOC_TERM = new Term(PRIME_DOC,PRIME_DOC_VALUE);
    
    private static Map<String,Map<String,Map<String,BlurBitSet>>> bitSets = new ConcurrentHashMap<String, Map<String,Map<String,BlurBitSet>>>();

    public interface IndexReaderCache {
        BlurBitSet getPrimeDocBitSet(SegmentReader reader);
    }
    
    public interface ShardCache {
        IndexReaderCache getIndexReaderCache(String shard);
    }
    
    public interface TableCache {
        ShardCache getShardCache(String table);
    }
    
    public static TableCache getTableCache() {
        return new TableCache() {
            @Override
            public ShardCache getShardCache(final String table) {
                checkTable(table);
                return new ShardCache() {
                    @Override
                    public IndexReaderCache getIndexReaderCache(final String shard) {
                        checkShard(table, shard);
                        return new IndexReaderCache() {
                            @Override
                            public BlurBitSet getPrimeDocBitSet(SegmentReader reader) {
                                return getBlurBitSet(table,shard,reader);
                            }
                        };
                    }
                };
            }
        };
    }
    
    private static BlurBitSet getBlurBitSet(String table, String shard, SegmentReader reader) {
        Map<String, Map<String, BlurBitSet>> shardMap = bitSets.get(table);
        if (shardMap == null) {
            throw new RuntimeException("While table is enabled, this shardMap should never be null.");
        }
        Map<String, BlurBitSet> segmentMap = shardMap.get(shard);
        String segmentName = reader.getSegmentName();
        BlurBitSet blurBitSet = segmentMap.get(segmentName);
        if (blurBitSet != null) {
            return blurBitSet;
        }
        return createBlurBitSet(reader,segmentMap);
    }
    
    private synchronized static BlurBitSet createBlurBitSet(SegmentReader reader, Map<String, BlurBitSet> segmentMap) {
        try {
            BlurBitSet bitSet = new BlurBitSet(reader.maxDoc());
            TermDocs termDocs = reader.termDocs(PRIME_DOC_TERM);
            while (termDocs.next()) {
                bitSet.set(termDocs.doc());
            }
            segmentMap.put(reader.getSegmentName(), bitSet);
            return bitSet;
        } catch (Exception e) {
            LOG.error("Error while trying to create prime doc bitset",e);
            throw new RuntimeException(e);
        }
    }

    private static void checkShard(String table, String shard) {
        Map<String, Map<String, BlurBitSet>> shardMap = bitSets.get(table);
        if (!shardMap.containsKey(shard)) {
            addShard(shard, shardMap);
        }
    }

    private static synchronized void addShard(String shard, Map<String, Map<String, BlurBitSet>> shardMap) {
        if (!shardMap.containsKey(shard)) {
            shardMap.put(shard, new ConcurrentHashMap<String, BlurBitSet>());
        }
    }
    
    protected static void checkTable(String table) {
        if (!bitSets.containsKey(table)) {
            addTable(table);
        }
    }

    private synchronized static void addTable(String table) {
        if (!bitSets.containsKey(table)) {
            bitSets.put(table, new ConcurrentHashMap<String, Map<String,BlurBitSet>>());
        }
    }
    
    
}
