package com.nearinfinity.blur.manager.indexserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.replication.LuceneIndexFileComparator;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory;
import com.nearinfinity.blur.store.replication.ReplicationDaemon;

public class HdfsIndexServer extends ManagedDistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(HdfsIndexServer.class);
    
    private FileSystem fileSystem;
    private Path blurBasePath;
    private Map<String,Map<String,Long>> cleanupMap = new ConcurrentHashMap<String, Map<String,Long>>();
    private LocalFileCache localFileCache;
    private LockFactory lockFactory;
    private ReplicationDaemon replicationDaemon;
    
    @Override
    public void close() {
        super.close();
        try {
            fileSystem.close();
        } catch (IOException e) {
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
        ReplicaHdfsDirectory directory = new ReplicaHdfsDirectory(table, shard, hdfsDirPath, fileSystem, localFileCache, lockFactory, new Progressable() {
            @Override
            public void progress() {
                //do nothing for now
            }
        }, replicationDaemon);
        touchFiles(directory,table,shard);
        return IndexReader.open(directory);
    }

    private void touchFiles(Directory directory, String table, String shard) throws IOException {
        LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
        List<String> list = new ArrayList<String>(Arrays.asList(directory.listAll()));
        Collections.sort(list,comparator);
        for (String f : list) {
            LOG.info("Touching file [" + f +
            		"] from table [" + table +
            		"] shard [" + shard + 
            		"]");
            IndexInput input = directory.openInput(f);
            if (input.length() > 0) {
                input.seek(0);
                input.readByte();
            }
            input.close();
        }
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

    public ReplicationDaemon getReplicationDaemon() {
        return replicationDaemon;
    }

    public void setReplicationDaemon(ReplicationDaemon replicationDaemon) {
        this.replicationDaemon = replicationDaemon;
    }
}
