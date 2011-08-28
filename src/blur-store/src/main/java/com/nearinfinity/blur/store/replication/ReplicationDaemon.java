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

package com.nearinfinity.blur.store.replication;

import static com.nearinfinity.blur.store.Constants.BUFFER_SIZE;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.indexinput.IndexInputFactory;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory.ReplicaIndexInput;

public class ReplicationDaemon implements Runnable {
    
    private static final Log LOG = LogFactory.getLog(ReplicationDaemon.class);
    private static final float MB = 1024 * 1024;
    
    public static class RepliaWorkUnit {
        ReplicaIndexInput replicaIndexInput;
        ReplicaHdfsDirectory directory;
    }
    
    static class RepliaWorkUnitCompartor implements Comparator<RepliaWorkUnit> {
        private LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
        @Override
        public int compare(RepliaWorkUnit o1, RepliaWorkUnit o2) {
            ReplicaIndexInput r1 = o1.replicaIndexInput;
            ReplicaIndexInput r2 = o2.replicaIndexInput;
            int compare = comparator.compare(r1.fileName, r2.fileName);
            if (compare == 0) {
                return r1.length < r2.length ? 1 : -1;
            }
            return compare;
        }
    } 
    
    private Thread _daemon;
    private LocalIOWrapper _wrapper = new LocalIOWrapper() {
        @Override
        public IndexOutput wrapOutput(IndexOutput fileIndexOutput) {
            return fileIndexOutput;
        }
        @Override
        public IndexInput wrapInput(IndexInput fileIndexInput) {
            return fileIndexInput;
        }
    };
    private long _period = TimeUnit.SECONDS.toMillis(1);
    private LocalFileCache _localFileCache;
    private PriorityBlockingQueue<RepliaWorkUnit> _replicaQueue = new PriorityBlockingQueue<RepliaWorkUnit>(1024, new RepliaWorkUnitCompartor());
    private Collection<String> _replicaNames = Collections.synchronizedCollection(new HashSet<String>());
    private volatile boolean _closed;
    private IndexInputFactory _indexInputFactory = new IndexInputFactory() {
        @Override
        public void replicationComplete(RepliaWorkUnit workUnit, LocalIOWrapper wrapper, int bufferSize) throws IOException {
            ReplicaIndexInput replicaIndexInput = workUnit.replicaIndexInput;
            ReplicaHdfsDirectory directory = workUnit.directory;
            String fileName = replicaIndexInput.fileName;
            AtomicReference<IndexInput> localInputRef = replicaIndexInput.localInput;
            
            IndexInput openFromLocal = directory.openFromLocal(fileName, BUFFER_SIZE);
            IndexInput localInput = wrapper.wrapInput(openFromLocal);
            localInputRef.set(localInput);
        }
    };
    
    public void init() {
        LOG.info("init - start");
        _daemon = new Thread(this);
        _daemon.setName("replication-daemon");
        _daemon.setDaemon(true);
        _daemon.setPriority(Thread.MIN_PRIORITY);
        _daemon.start();
        LOG.info("init - complete");
    }
    
    public synchronized void close() {
        if (!_closed) {
            _closed = true;
            _daemon.interrupt();
        }
    }

    @Override
    public void run() {
        while (!_closed) {
            try {
                replicate();
            } catch (Exception e) {
                LOG.error("Error during local replication.", e);
            }
            try {
                Thread.sleep(_period);
            } catch (InterruptedException e) {
                if (_closed) {
                    return;
                }
                throw new RuntimeException(e);
            }
        }
    }

    private void replicate() throws InterruptedException, IOException {
        while (!_replicaQueue.isEmpty() && !_closed) {
            LOG.info("Total files left to be replicated [{0}], totaling [{1} MB]",getNumberOfFilesToReplicate(),getSizeOfFilesToReplicate());
            RepliaWorkUnit unit = _replicaQueue.take();
            ReplicaIndexInput replicaIndexInput = unit.replicaIndexInput;
            String dirName = replicaIndexInput.dirName;
            String fileName = replicaIndexInput.fileName;
            ReplicaHdfsDirectory directory = unit.directory;
            FileSystem fileSystem = directory.getFileSystem();
            Path hdfsDirPath = directory.getHdfsDirPath();
            
            File localFile = _localFileCache.getLocalFile(dirName, fileName);
            if (localFile.exists()) {
                LOG.info("Local file of [{0}/{1}] was found, deleting and recopying.",dirName,fileName);
                if (!localFile.delete()) {
                    LOG.fatal("Error trying to delete existing file during replication [{0}]", localFile.getAbsolutePath());
                    //not sure what to do now.
                } else {
                    localFile = _localFileCache.getLocalFile(dirName, fileName);
                }
            }
            
            Path source = new Path(hdfsDirPath,fileName);
            Path dest = new Path(localFile.toURI().toString());
            LOG.info("Copying file [{0}/{1}] locally.",dirName,fileName);
            fileSystem.copyToLocalFile(source, dest);
            LOG.info("Finished copying file [{0}/{1}] locally.",dirName,fileName);
            
            _indexInputFactory.replicationComplete(unit, _wrapper, BUFFER_SIZE);
            _replicaNames.remove(getLookupName(dirName,fileName));
        }
    }

    private int getNumberOfFilesToReplicate() {
        return _replicaQueue.size();
    }

    private float getSizeOfFilesToReplicate() {
        float total = 0;
        for (RepliaWorkUnit unit : _replicaQueue) {
            total += (unit.replicaIndexInput.length / MB);
        }
        return total;
    }

    public void replicate(ReplicaHdfsDirectory directory, ReplicaIndexInput replicaIndexInput) {
        String dirName = replicaIndexInput.dirName;
        String fileName = replicaIndexInput.fileName;
        if (isBeingReplicated(dirName,fileName)) {
            return;
        }
        RepliaWorkUnit unit = new RepliaWorkUnit();
        unit.replicaIndexInput = replicaIndexInput;
        unit.directory = directory;
        _replicaQueue.add(unit);
        _replicaNames.add(getLookupName(dirName,fileName));
    }

    private String getLookupName(String dirName, String fileName) {
        return dirName + "~" + fileName;
    }

    public boolean isBeingReplicated(String dirName, String name) {
        return _replicaNames.contains(getLookupName(dirName, name));
    }

    public void setWrapper(LocalIOWrapper wrapper) {
        this._wrapper = wrapper;
    }

    public void setLocalFileCache(LocalFileCache localFileCache) {
        this._localFileCache = localFileCache;
    }
}
