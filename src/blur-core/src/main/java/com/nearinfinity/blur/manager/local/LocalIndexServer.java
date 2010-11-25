package com.nearinfinity.blur.manager.local;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Selector;

public class LocalIndexServer implements IndexServer {
    
    private Map<String,Map<String, IndexReader>> readersMap = new ConcurrentHashMap<String, Map<String,IndexReader>>();
    private File localDir;
    
    public LocalIndexServer(File file) {
        this.localDir = file;
    }

    @Override
    public Analyzer getAnalyzer(String table) {
        return new StandardAnalyzer(Version.LUCENE_30);
    }

    @Override
    public IndexReader getIndexReader(String table, Selector selector) throws IOException, MissingShardException {
        Map<String, IndexReader> indexReaders = getIndexReaders(table);
        String shard = getShard(selector.getLocationId());
        IndexReader indexReader = indexReaders.get(shard);
        if (indexReader == null) {
            throw new MissingShardException("Shard [" + shard +
            		"] not found in table [" + table +
            		"]");
        }
        return indexReader;
    }

    /**
     * Location id format is <shard>/luceneid.
     * @param locationId
     * @return
     */
    private String getShard(String locationId) {
        String[] split = locationId.split("\\/");
        if (split.length != 2) {
            throw new IllegalArgumentException("Location id invalid [" + locationId + "]");
        }
        return split[0];
    }

    @Override
    public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
        Map<String, IndexReader> tableMap = readersMap.get(table);
        if (tableMap == null) {
            tableMap = openFromDisk(table);
            readersMap.put(table, tableMap);
        }
        return tableMap;
    }

    private Map<String, IndexReader> openFromDisk(String table) throws IOException {
        File tableFile = new File(localDir,table);
        if (tableFile.isDirectory()) {
            Map<String, IndexReader> shards = new ConcurrentHashMap<String, IndexReader>();
            for (File f : tableFile.listFiles()) {
                if (f.isDirectory()) {
                    shards.put(f.getName(),openReader(f));
                }
            }
            return shards;
        }
        throw new IOException("Table [" + table + "] not found.");
    }

    private IndexReader openReader(File f) throws CorruptIndexException, IOException {
        return IndexReader.open(FSDirectory.open(f));
    }

    @Override
    public Similarity getSimilarity() {
        return new FairSimilarity();
    }
    
    @Override
    public void close() {
        
    }
}
