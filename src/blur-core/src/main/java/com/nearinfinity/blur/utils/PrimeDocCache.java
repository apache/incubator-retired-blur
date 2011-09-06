/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.util.OpenBitSet;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class PrimeDocCache {
    
    private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);

    public static final OpenBitSet EMPTY_BIT_SET = new OpenBitSet();
    private static final Term PRIME_DOC_TERM = new Term(PRIME_DOC,PRIME_DOC_VALUE);
    
    private static Map<String,Map<String,Map<String,OpenBitSet>>> bitSets = new ConcurrentHashMap<String, Map<String,Map<String,OpenBitSet>>>();

    public interface IndexReaderCache {
        OpenBitSet getPrimeDocBitSet(SegmentReader reader);
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
                            public OpenBitSet getPrimeDocBitSet(SegmentReader reader) {
                                return getBlurBitSet(table,shard,reader);
                            }
                        };
                    }
                };
            }
        };
    }
    
    private static OpenBitSet getBlurBitSet(String table, String shard, SegmentReader reader) {
        Map<String, Map<String, OpenBitSet>> shardMap = bitSets.get(table);
        if (shardMap == null) {
            throw new RuntimeException("While table is enabled, this shardMap should never be null.");
        }
        Map<String, OpenBitSet> segmentMap = shardMap.get(shard);
        String segmentName = reader.getSegmentName();
        OpenBitSet blurBitSet = segmentMap.get(segmentName);
        if (blurBitSet != null) {
            return blurBitSet;
        }
        return createBlurBitSet(reader,segmentMap);
    }
    
    private synchronized static OpenBitSet createBlurBitSet(SegmentReader reader, Map<String, OpenBitSet> segmentMap) {
        try {
            OpenBitSet bitSet = new OpenBitSet(reader.maxDoc());
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
        Map<String, Map<String, OpenBitSet>> shardMap = bitSets.get(table);
        if (!shardMap.containsKey(shard)) {
            addShard(shard, shardMap);
        }
    }

    private static synchronized void addShard(String shard, Map<String, Map<String, OpenBitSet>> shardMap) {
        if (!shardMap.containsKey(shard)) {
            shardMap.put(shard, new ConcurrentHashMap<String, OpenBitSet>());
        }
    }
    
    protected static void checkTable(String table) {
        if (!bitSets.containsKey(table)) {
            addTable(table);
        }
    }

    private synchronized static void addTable(String table) {
        if (!bitSets.containsKey(table)) {
            bitSets.put(table, new ConcurrentHashMap<String, Map<String,OpenBitSet>>());
        }
    }
    
    
}
