package com.nearinfinity.blur.utils;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections.map.AbstractReferenceMap;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.utils.bitset.BlurBitSet;

@SuppressWarnings("unchecked")
public class PrimeDocCache {
    
    public static final BlurBitSet EMPTY_BIT_SET = new BlurBitSet();
    private static final Log LOG = LogFactory.getLog(PrimeDocCache.class);
    private static Map<IndexReader, BlurBitSet> primeDocs = Collections.synchronizedMap(new ReferenceMap(
            AbstractReferenceMap.SOFT, AbstractReferenceMap.HARD));

    public static BlurBitSet getPrimeDoc(IndexReader reader) {
        BlurBitSet blurBitSet = primeDocs.get(reader);
        if (blurBitSet == null) {
            LOG.error("Empty Prime Doc BitSet for [" + reader + "]");
            return EMPTY_BIT_SET;
        }
        return blurBitSet;
    }

    public static void setPrimeDoc(IndexReader reader, BlurBitSet bitSet) {
        primeDocs.put(reader, bitSet);
    }

    public static boolean isPrimeDocPopulated(IndexReader reader) {
        return primeDocs.containsKey(reader);
    }

}
