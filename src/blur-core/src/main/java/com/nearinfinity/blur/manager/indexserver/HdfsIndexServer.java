package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.lucene.store.NoLockFactory;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.lucene.search.FairSimilarity;
import com.nearinfinity.blur.lucene.store.HdfsDirectory;
import com.nearinfinity.blur.lucene.store.LocalReplicaDirectoryForReading;
import com.nearinfinity.blur.lucene.store.MixedFSDirectory;

public class HdfsIndexServer extends ManagedDistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(HdfsIndexServer.class);
    
    private FileSystem fileSystem;
    private Path blurBasePath;
    private Map<String,Analyzer> tableAnalyzers = new ConcurrentHashMap<String, Analyzer>();
    private List<File> localFileCaches;

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
                LOG.info("Table [" + table +
                        "] status [" + TABLE_STATUS.ENABLED +
                        "]");
                return TABLE_STATUS.ENABLED;
            }
            LOG.info("Table [" + table +
                    "] status [" + TABLE_STATUS.DISABLED +
                    "]");
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
}
