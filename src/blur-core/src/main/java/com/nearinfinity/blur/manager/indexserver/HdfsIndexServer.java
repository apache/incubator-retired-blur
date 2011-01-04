package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.NoLockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.lucene.store.HdfsDirectory;
import com.nearinfinity.blur.lucene.store.LocalReplicaDirectoryForReading;
import com.nearinfinity.blur.lucene.store.MixedFSDirectory;

public class HdfsIndexServer extends ManagedDistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(HdfsIndexServer.class);
    private static final long TRASH_CAN_TIME = TimeUnit.MINUTES.toMillis(10);
    
    private FileSystem fileSystem;
    private Path blurBasePath;
    private Map<String,Analyzer> tableAnalyzers = new ConcurrentHashMap<String, Analyzer>();
    private List<File> localFileCaches;
    private Timer cleanup;
    private long cleanupDelay = TimeUnit.SECONDS.toMillis(10);
    private Map<String,Map<String,Long>> cleanupMap = new ConcurrentHashMap<String, Map<String,Long>>();
    private Map<String,TABLE_STATUS> enabledTables = new ConcurrentHashMap<String, TABLE_STATUS>();
    
    @Override
    public HdfsIndexServer init() {
        super.init();
        startLocalIndexCleanUpDaemon();
        return this;
    }

    private void startLocalIndexCleanUpDaemon() {
        cleanup = new Timer("Local Index Cleanup", true);
        cleanup.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    performCleanup();
                } catch (IOException e) {
                    LOG.error("Unknown error during cleanup of old local indexes.",e);
                }
            }
        }, cleanupDelay, cleanupDelay);
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
        LOG.info("Opening shard [" + shard +
        		"] for table [" + table +
        		"]");
        Path tablePath = new Path(blurBasePath,table);
        if (!exists(tablePath)) {
            throw new FileNotFoundException(tablePath.toString());
        }
        Path hdfsDirPath = new Path(tablePath,shard);
        if (!exists(hdfsDirPath)) {
            throw new FileNotFoundException(hdfsDirPath.toString());
        }
        File localFileForCache = getLocalFileForCache(table,shard);
        LOG.info("Local dir for caching located [" + localFileForCache + "]");
        HdfsDirectory hdfsDir = new HdfsDirectory(hdfsDirPath, fileSystem);
        MixedFSDirectory directory = new MixedFSDirectory(localFileForCache, new NoLockFactory());
        LocalReplicaDirectoryForReading localReplicaDirectory = new LocalReplicaDirectoryForReading(directory, hdfsDir, directory.getLockFactory());
        return IndexReader.open(localReplicaDirectory);
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
    
    private void performCleanup() throws IOException {
        LOG.debug("Performing local cleanup");
        for (String table : new TreeSet<String>(cleanupMap.keySet())) {
            LOG.info("Performing local cleanup table [" + table + "]");
            Map<String, Long> map = cleanupMap.get(table);
            if (map != null) {
                for (String shard : new TreeSet<String>(map.keySet())) {
                    LOG.info("Performing local cleanup table [" + table + "] shard [" + shard + "]");
                    Map<String, IndexReader> indexReaders = getIndexReaders(table);
                    if (!indexReaders.keySet().contains(shard)) {
                        long timestamp = map.get(shard);
                        if (timestamp + TRASH_CAN_TIME < System.currentTimeMillis()) {
                            deleteLocalDir(table,shard);
                            map.remove(shard);
                        }
                    } else {
                        LOG.info("Local cleanup NOT NEEDED for table [" + table + "] shard [" + shard + "]");
                        map.remove(shard);
                    }
                }
            }
        }
    }

    private void deleteLocalDir(String table, String shard) {
        LOG.info("Deleting local table [" + table + "] shard [" + shard + "]");
        File localCache = locateLocalCache(table, shard);
        File[] listFiles = localCache.listFiles();
        File delFolder = new File(localCache,".trash");
        delFolder.mkdirs();
        for (File f : listFiles) {
            File dest = new File(delFolder,f.getName());
            f.renameTo(dest);
            LOG.info("Moving file [" + f.getAbsolutePath() +
            		"] to [" + dest.getAbsolutePath() +
            		"]");
        }
        backgroundDelete(table, shard, delFolder);
    }

    private void backgroundDelete(String table, String shard, final File delFolder) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                rm(delFolder);
            }
        });
        thread.setName("DELETING-" + table + "/" + shard +"/.trash");
        thread.setDaemon(true);
        thread.start();
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

    public List<File> getLocalFileCaches() {
        return localFileCaches;
    }

    public void setLocalFileCaches(List<File> localFileCaches) {
        this.localFileCaches = localFileCaches;
    }
    
    private File getLocalFileForCache(String table, String shard) throws IOException {
        File file = locateLocalCache(table,shard);
        if (file != null) {
            return file;
        }
        return createNewLocalCache(table,shard);
    }

    private File createNewLocalCache(String table, String shard) throws IOException {
        List<File> localPaths = new ArrayList<File>(localFileCaches);
        Collections.shuffle(localPaths);
        
        for (File file : localPaths) {
            if (isGoodDisk(file)) {
                File cacheDir = new File(new File(file,table),shard);
                cacheDir.mkdirs();
                return cacheDir;
            }
        }
        
        throw new IOException("No local disks available.");
    }

    private boolean isGoodDisk(File file) {
        if (!file.exists()) {
            if (!file.mkdirs()) {
                return false;
            }
        } else if (!file.isDirectory()) {
            return false;
        }
        
        File testFile = new File(file,"testing.tmp");
        try {
            if (!testFile.createNewFile()) {
                return false;
            }
        } catch (IOException e) {
            return false;
        } finally {
            testFile.delete();
        }
        return true;
    }
    
    private File locateLocalCache(String table, String shard) {
        for (File dir : localFileCaches) {
            if (isGoodDisk(dir)) {
                File indexDir = new File(new File(dir,table),shard);
                if (indexDir.exists()) {
                    return indexDir;
                }
            }
        }
        return null;
    }

    private boolean exists(Path path) throws IOException {
        return fileSystem.exists(path);
    }
}
