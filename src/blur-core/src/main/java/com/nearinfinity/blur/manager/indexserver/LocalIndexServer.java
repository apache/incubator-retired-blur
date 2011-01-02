package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.mele.util.AddressUtil;

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
    public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
        Map<String, IndexReader> tableMap = readersMap.get(table);
        if (tableMap == null) {
            tableMap = openFromDisk(table);
            readersMap.put(table, tableMap);
        }
        return tableMap;
    }

    @Override
    public Similarity getSimilarity(String table) {
        return new FairSimilarity();
    }
    
    @Override
    public void close() {
        
    }
    
    private Map<String, IndexReader> openFromDisk(String table) throws IOException {
        File tableFile = new File(localDir,table);
        if (tableFile.isDirectory()) {
            Map<String, IndexReader> shards = new ConcurrentHashMap<String, IndexReader>();
            for (File f : tableFile.listFiles()) {
                if (f.isDirectory()) {
                    Directory directory = FSDirectory.open(f);
                    if (IndexReader.indexExists(directory)) {
                        shards.put(f.getName(),openReader(directory));
                    } else {
                        directory.close();
                    }
                }
            }
            return shards;
        }
        throw new IOException("Table [" + table + "] not found.");
    }

    private IndexReader openReader(Directory dir) throws CorruptIndexException, IOException {
        return IndexReader.open(dir);
    }

    @Override
    public TABLE_STATUS getTableStatus(String table) {
        return TABLE_STATUS.ENABLED;
    }

    @Override
    public List<String> getControllerServerList() {
        try {
            return Arrays.asList(AddressUtil.getMyHostName() + ":40010");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getShardServerList() {
        try {
            return Arrays.asList(AddressUtil.getMyHostName() + ":40020");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getTableList() {
        return new ArrayList<String>(readersMap.keySet());
    }

    @Override
    public List<String> getShardList(String table) {
        try {
            List<String> result = new ArrayList<String>();
            File tableFile = new File(localDir,table);
            if (tableFile.isDirectory()) {
                for (File f : tableFile.listFiles()) {
                    if (f.isDirectory()) {
                        Directory directory = FSDirectory.open(f);
                        if (IndexReader.indexExists(directory)) {
                            result.add(f.getName());
                        }
                        directory.close();
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getOfflineShardServers() {
        return new ArrayList<String>();
    }
    
    @Override
    public List<String> getOnlineShardServers() {
        return getShardServerList();
    }
}
