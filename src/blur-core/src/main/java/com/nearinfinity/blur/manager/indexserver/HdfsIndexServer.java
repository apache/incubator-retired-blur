/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.indexserver;

import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC;
import static com.nearinfinity.blur.utils.BlurConstants.PRIME_DOC_VALUE;
import static com.nearinfinity.blur.utils.BlurConstants.SHARD_PREFIX;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.manager.writer.BlurIndexCloser;
import com.nearinfinity.blur.manager.writer.BlurIndexCommiter;
import com.nearinfinity.blur.manager.writer.BlurIndexRefresher;
import com.nearinfinity.blur.manager.writer.BlurIndexWriter;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.lock.ZookeeperLockFactory;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory;
import com.nearinfinity.blur.store.replication.ReplicationDaemon;
import com.nearinfinity.blur.store.replication.ReplicationStrategy;
import com.nearinfinity.lucene.compressed.CompressedFieldDataDirectory;

public class HdfsIndexServer extends ManagedDistributedIndexServer {
    
    private static final Log LOG = LogFactory.getLog(HdfsIndexServer.class);

    private LocalFileCache _localFileCache;
    private ReplicationDaemon _replicationDaemon;
    private boolean _closed;
    private ReplicationStrategy _replicationStrategy;
    private Configuration _configuration = new Configuration();
    private BlurIndexCloser _closer;
    private ZooKeeper _zookeeper;
    private BlurIndexRefresher _refresher;
    private BlurIndexCommiter _commiter;
    
    @Override
    public void init() {
        LOG.info("init - start");
        super.init();
        _closer = new BlurIndexCloser();
        _closer.init();
        LOG.info("init - complete");
    }
    
    @Override
    public synchronized void close() {
        _closer.close();
        if (!_closed) {
            _closed = true;
            super.close();
        }
    }

    @Override
    protected BlurIndex openShard(String table, String shard) throws IOException {
        LOG.info("Opening shard [{0}] for table [{1}]",shard,table);
        URI tableUri = getTableURI(table);
        Path tablePath = new Path(tableUri.toString());
        Path hdfsDirPath = new Path(tablePath,shard);
        
        String shardPath = ZookeeperPathConstants.getBlurLockPath(table) + "/" + shard;
        ZookeeperLockFactory lockFactory = new ZookeeperLockFactory(_zookeeper, shardPath);
        ReplicaHdfsDirectory directory = new ReplicaHdfsDirectory(table, shard, hdfsDirPath, 
                _localFileCache, lockFactory, new Progressable() {
            @Override
            public void progress() {
                //do nothing for now
            }
        }, _replicationDaemon, _replicationStrategy);
        
        CompressedFieldDataDirectory compressedDirectory = new CompressedFieldDataDirectory(directory, 
                getCompressionCodec(table), 
                getCompressionBlockSize(table));
        
        BlurIndexWriter writer = new BlurIndexWriter();
        writer.setCloser(_closer);
        writer.setCommiter(_commiter);
        writer.setAnalyzer(getAnalyzer(table));
        writer.setDirectory(compressedDirectory);
        writer.setRefresher(_refresher);
        writer.init();
        return warmUp(writer);
    }

    private URI getTableURI(String table) {
        try {
            return new URI(getTableUri(table));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private BlurIndex warmUp(BlurIndex index) throws IOException {
        IndexReader reader = index.getIndexReader(true);
        try {
            warmUpAllSegments(reader);
        } finally {
            //this will allow for closing of index
            reader.decRef();
        }
        return index;
    }

    private void warmUpAllSegments(IndexReader reader) throws IOException {
        IndexReader[] indexReaders = reader.getSequentialSubReaders();
        if (indexReaders != null) {
            for (IndexReader r : indexReaders) {
                warmUpAllSegments(r);
            }
        }
        int maxDoc = reader.maxDoc();
        int numDocs = reader.numDocs();
        Collection<String> fieldNames = reader.getFieldNames(FieldOption.ALL);
        Term term = new Term(PRIME_DOC,PRIME_DOC_VALUE);
        int primeDocCount = reader.docFreq(term);
        
        TermDocs termDocs = reader.termDocs(term);
        termDocs.next();
        termDocs.close();
        
        TermPositions termPositions = reader.termPositions(term);
        if (termPositions.next()) {
            if (termPositions.freq() > 0) {
                termPositions.nextPosition();
            }
        }
        termPositions.close();
        LOG.info("Warmup of indexreader [" + reader + "] complete, maxDocs [" + maxDoc + "], numDocs [" + numDocs + "], primeDocumentCount [" + primeDocCount + "], fieldCount [" + fieldNames.size() + "]");
    }

    @Override
    public List<String> getShardList(String table) {
        List<String> result = new ArrayList<String>();
        try {
            URI tableUri = getTableURI(table);
            Path tablePath = new Path(tableUri.toString());
            FileSystem fileSystem = FileSystem.get(tableUri, _configuration);
            if (!fileSystem.exists(tablePath)) {
                LOG.warn("Table [{0}] is missing, defined location [{1}]",table,tableUri.toString());
                return new ArrayList<String>();
            }
            FileStatus[] listStatus = fileSystem.listStatus(tablePath);
            for (FileStatus status : listStatus) {
                if (status.isDir()) {
                    String name = status.getPath().getName();
                    if (name.startsWith(SHARD_PREFIX)) {
                        result.add(name);
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void beforeClose(String shard, BlurIndex indexReader) {

    }
    
    protected static void rm(File file) {
        LOG.info("Deleting file [{0}]",file);
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }
    
    public void setLocalFileCache(LocalFileCache localFileCache) {
        this._localFileCache = localFileCache;
    }

    public void setReplicationDaemon(ReplicationDaemon replicationDaemon) {
        this._replicationDaemon = replicationDaemon;
    }

    public void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
        this._replicationStrategy = replicationStrategy;
    }

    public void setZookeeper(ZooKeeper zookeeper) {
        _zookeeper = zookeeper;
    }

	@Override
	public long getTableSize(String table) throws IOException {
		Path tablePath = new Path(getTableUri(table));
		FileSystem fileSystem = FileSystem.get(tablePath.toUri(), _configuration);
		return fileSystem.getFileStatus(tablePath).getLen();
	}

    public void setRefresher(BlurIndexRefresher refresher) {
        _refresher = refresher;
    }

    public void setCommiter(BlurIndexCommiter commiter) {
        _commiter = commiter;
    }

}
