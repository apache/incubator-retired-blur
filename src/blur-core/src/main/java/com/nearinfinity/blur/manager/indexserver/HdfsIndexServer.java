package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;

public class HdfsIndexServer extends ZookeeperManagedDistributedIndexServer {
    
    private FileSystem fileSystem;
    private Path blurBasePath;
    private Map<String,Analyzer> tableAnalyzers = new ConcurrentHashMap<String, Analyzer>();

    @Override
    public List<String> getTableList() {
        List<String> result = new ArrayList<String>();
        try {
            FileStatus[] listStatus = fileSystem.listStatus(blurBasePath);
            for (FileStatus status : listStatus) {
                if (status.isDir()) {
                    result.add(status.getPath().getName());
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Similarity getSimilarity(String table) {
        return new FairSimilarity();
    }
    
    @Override
    public Analyzer getAnalyzer(String table) {
        Analyzer analyzer = tableAnalyzers.get(table);
        if (analyzer == null) {
            return loadAnalyzer(table);
        }
        return analyzer;
    }

    @Override
    public TABLE_STATUS getTableStatus(String table) {
        Path tablePath = new Path(blurBasePath,table);
        try {
            if (fileSystem.exists(new Path(tablePath,"enabled"))) {
                return TABLE_STATUS.ENABLED;
            }
            return TABLE_STATUS.DISABLED;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void close() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized Analyzer loadAnalyzer(String table) {
        Path tablePath = new Path(blurBasePath,table);
        try {
            FSDataInputStream inputStream = fileSystem.open(new Path(tablePath,"analyzer.json"));
            BlurAnalyzer analyzer = BlurAnalyzer.create(inputStream);
            tableAnalyzers.put(table, analyzer);
            return analyzer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected IndexReader openShard(String table, String shard) {
        return null;
    }

    @Override
    public List<String> getShardList(String table) {
        return null;
    }

    @Override
    protected void beforeClose(String shard, IndexReader indexReader) {
        
    }
}
