package com.nearinfinity.blur.manager.hits;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.utils.BlurConstants;

public class HitsIterableSearcher implements HitsIterable, BlurConstants {
    
    private static final Log LOG = LogFactory.getLog(HitsIterableSearcher.class);

    private Map<String, Long> shardInfo = new TreeMap<String, Long>();
    private Query query;
    private String shard;
    private IndexSearcher searcher;
    private long skipTo;
    private long totalHits;
    private TopDocs topDocs;
    private int fetchCount = 1000;
    private int batch = 0;
    private String commitPoint;

    public HitsIterableSearcher(Query query, String shard, IndexSearcher searcher, IndexCommit indexCommit) throws IOException {
        this.query = query;
        this.shard = shard;
        this.searcher = searcher;
        Map<String, String> userData = indexCommit.getUserData();
        this.commitPoint = userData.get(COMMIT_POINT);
        performSearch();
    }

    private void performSearch() {
        try {
            topDocs = searcher.search(query, fetchCount * (batch + 1));
            totalHits = topDocs.totalHits;
            shardInfo.put(shard, totalHits);
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
        return docId + "@" + shard + "." + commitPoint;
    }  
}
