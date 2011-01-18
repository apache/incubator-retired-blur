package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.store.LocalFileCache;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory;

public class HdfsIndexServer extends ManagedDistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(HdfsIndexServer.class);
    
    private FileSystem fileSystem;
    private Path blurBasePath;
    private Map<String,Analyzer> tableAnalyzers = new ConcurrentHashMap<String, Analyzer>();
    private Map<String,Map<String,Long>> cleanupMap = new ConcurrentHashMap<String, Map<String,Long>>();
    private Map<String,TABLE_STATUS> enabledTables = new ConcurrentHashMap<String, TABLE_STATUS>();
    private LocalFileCache localFileCache;
    private LockFactory lockFactory;
    
    @Override
    public HdfsIndexServer init() {
        super.init();
        return this;
    }

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
        TABLE_STATUS status = enabledTables.get(table);
        if (status != null) {
            return status;
        }
        return readTableStatus(table);
    }
    
    private synchronized TABLE_STATUS readTableStatus(String table) {
        TABLE_STATUS status = enabledTables.get(table);
        if (status != null) {
            return status;
        }
        Path tablePath = new Path(blurBasePath,table);
        try {
            if (fileSystem.exists(new Path(tablePath,"enabled"))) {
                LOG.info("Table [" + table +
                        "] status [" + TABLE_STATUS.ENABLED +
                        "]");
                return recordTableStatus(table,TABLE_STATUS.ENABLED);
            }
            LOG.info("Table [" + table +
                    "] status [" + TABLE_STATUS.DISABLED +
                    "]");
            return recordTableStatus(table,TABLE_STATUS.DISABLED);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TABLE_STATUS recordTableStatus(String table, TABLE_STATUS status) {
        enabledTables.put(table, status);
        return status;
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
    protected IndexReader openShard(String table, String shard) throws IOException {
        LOG.info("Opening shard [" + shard + "] for table [" + table + "]");
        Path tablePath = new Path(blurBasePath,table);
        if (!exists(tablePath)) {
            throw new FileNotFoundException(tablePath.toString());
        }
        Path hdfsDirPath = new Path(tablePath,shard);
        if (!exists(hdfsDirPath)) {
            throw new FileNotFoundException(hdfsDirPath.toString());
        }
        ReplicaHdfsDirectory directory = new ReplicaHdfsDirectory(table + "__" + shard, hdfsDirPath, fileSystem, localFileCache, lockFactory);
        return IndexReader.open(directory);
        
    }

    @Override
    public List<String> getShardList(String table) {
        List<String> result = new ArrayList<String>();
        try {
            FileStatus[] listStatus = fileSystem.listStatus(new Path(blurBasePath,table));
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
    protected void beforeClose(String shard, IndexReader indexReader) {
        
    }
    
    @Override
    protected synchronized void cleanupLocallyCachedIndexes(String table, String shard) {
        LOG.info("Local cleanup added for table [" + table +
        		"] shard [" + shard + 
        		"]");
        Map<String, Long> map = cleanupMap.get(table);
        if (map == null) {
            map = new ConcurrentHashMap<String, Long>();
            cleanupMap.put(table, map);
        }
        map.put(shard, System.currentTimeMillis());
    }
    
    protected static void rm(File file) {
        LOG.info("Deleting file [" + file + "]");
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public Path getBlurBasePath() {
        return blurBasePath;
    }

    public void setBlurBasePath(Path blurBasePath) {
        this.blurBasePath = blurBasePath;
    }

    private boolean exists(Path path) throws IOException {
        return fileSystem.exists(path);
    }

    public LocalFileCache getLocalFileCache() {
        return localFileCache;
    }

    public void setLocalFileCache(LocalFileCache localFileCache) {
        this.localFileCache = localFileCache;
    }

    public LockFactory getLockFactory() {
        return lockFactory;
    }

    public void setLockFactory(LockFactory lockFactory) {
        this.lockFactory = lockFactory;
    }
}
