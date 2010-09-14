package com.nearinfinity.blur.manager.hits;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.MapFieldSelector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.nearinfinity.blur.lucene.index.SuperDocument;
import com.nearinfinity.blur.thrift.generated.Hit;

public class SearchHitsIterable implements HitsIterable {
    
    private static final Log LOG = LogFactory.getLog(SearchHitsIterable.class);
    private static final MapFieldSelector SELECTOR = new MapFieldSelector(Arrays.asList(SuperDocument.ID,SuperDocument.SUPER_KEY));

    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private Query query;
    private String shard;
    private IndexSearcher searcher;
    private long skipTo;
    private long totalHits;
    private TopDocs topDocs;
    private int fetchCount = 100;
    private int batch = 0;
    private boolean superOn;

    public SearchHitsIterable(boolean superOn, Query query, String shard, IndexSearcher searcher) {
        this.superOn = superOn;
        this.query = query;
        this.shard = shard;
        this.searcher = searcher;
        performSearch();
    }

    private void performSearch() {
        try {
            topDocs = searcher.search(query, fetchCount * (batch + 1));
            totalHits = topDocs.totalHits;
            batch++;
        } catch (IOException e) {
            LOG.error("Error during for [" + query +
            		"] on shard [" + shard + 
            		"] with fetch count [" + fetchCount +
            		"]",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Long> getShardInfo() {
        return shardInfo;
    }

    @Override
    public long getTotalHits() {
        return totalHits;
    }

    @Override
    public void skipTo(long skipTo) {
        this.skipTo = skipTo;
    }

    @Override
    public Iterator<Hit> iterator() {
        SearchIterator iterator = new SearchIterator();
        long start = 0;
        while (iterator.hasNext() && start < skipTo) {
            iterator.next();
            start++;
        }
        return iterator;
    }
    
    public class SearchIterator implements Iterator<Hit> {
        
        private int position = 0;

        @Override
        public boolean hasNext() {
            if (position < totalHits) {
                return true;
            }
            return false;
        }

        @Override
        public Hit next() {
            if (position >= topDocs.scoreDocs.length) {
                performSearch();
            }
            ScoreDoc scoreDoc = topDocs.scoreDocs[position++];
            return new Hit(resolveId(scoreDoc.doc), scoreDoc.score, "UNKNOWN");
        }

        @Override
        public void remove() {
            
        }
    }
    
    private String resolveId(int docId) {
        Document doc;
        try {
            doc = searcher.doc(docId, SELECTOR);
        } catch (IOException e) {
            LOG.error("Error while trying to fetch actual row ids from lucene doc id [" + docId +
            		"]");
            throw new RuntimeException();
        }
        String id = doc.get(SuperDocument.ID);
        if (superOn) {
            return id;
        }
        String superColumnid = doc.get(SuperDocument.SUPER_KEY);
        return id + ":" + superColumnid;
    }  
}
