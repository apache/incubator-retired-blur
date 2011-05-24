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
    
    private Thread daemon;
    private LocalIOWrapper wrapper = new LocalIOWrapper() {
        @Override
        public IndexOutput wrapOutput(IndexOutput fileIndexOutput) {
            return fileIndexOutput;
        }
        @Override
        public IndexInput wrapInput(IndexInput fileIndexInput) {
            return fileIndexInput;
        }
    };
    private long period = TimeUnit.SECONDS.toMillis(1);
    private LocalFileCache localFileCache;
    private PriorityBlockingQueue<RepliaWorkUnit> replicaQueue = new PriorityBlockingQueue<RepliaWorkUnit>(1024, new RepliaWorkUnitCompartor());
    private Collection<String> replicaNames = Collections.synchronizedCollection(new HashSet<String>());
    private volatile boolean closed;
    private IndexInputFactory indexInputFactory = new IndexInputFactory() {
        @Override
        public void replicationComplete(RepliaWorkUnit workUnit, LocalIOWrapper wrapper, int bufferSize) throws IOException {
            IndexInput localInput = wrapper.wrapInput(workUnit.directory.openFromLocal(workUnit.replicaIndexInput.fileName, BUFFER_SIZE));
            workUnit.replicaIndexInput.localInput.set(localInput);
        }
    };
    
    public void init() {
        this.daemon = new Thread(this);
        this.daemon.setDaemon(true);
        this.daemon.setPriority(Thread.MIN_PRIORITY);
        this.daemon.start();
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;
            daemon.interrupt();
        }
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                replicate();
            } catch (Exception e) {
                LOG.error("Error during local replication.", e);
            }
            try {
                Thread.sleep(period);
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
                throw new RuntimeException(e);
            }
        }
    }

    private void replicate() throws InterruptedException, IOException {
        RepliaWorkUnit workUnit = null;
        while (!replicaQueue.isEmpty() && !closed) {
            RepliaWorkUnit unit = replicaQueue.take();
            ReplicaIndexInput replicaIndexInput = unit.replicaIndexInput;
            String dirName = replicaIndexInput.dirName;
            String fileName = replicaIndexInput.fileName;
            ReplicaHdfsDirectory directory = unit.directory;
            FileSystem fileSystem = directory.getFileSystem();
            Path hdfsDirPath = directory.getHdfsDirPath();
            
            File localFile = localFileCache.getLocalFile(dirName, fileName);
            if (localFile.exists()) {
                LOG.info("Local file of [{0}/{1}] was found, deleting and recopying.",dirName,fileName);
                if (!localFile.delete()) {
                    LOG.fatal("Error trying to delete existing file during replication [{0}]", localFile.getAbsolutePath());
                } else {
                    localFile = localFileCache.getLocalFile(dirName, fileName);
                }
            }
            
            Path source = new Path(hdfsDirPath,fileName);
            Path dest = new Path(localFile.toURI().toString());
            LOG.info("Copying file [{0}/{1}] locally.",dirName,fileName);
            fileSystem.copyToLocalFile(source, dest);
            LOG.info("Finished copying file [{0}/{1}] locally.",dirName,fileName);
            
            indexInputFactory.replicationComplete(workUnit, wrapper, BUFFER_SIZE);
            replicaNames.remove(getLookupName(dirName,fileName));
        }
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
        replicaQueue.add(unit);
        replicaNames.add(getLookupName(dirName,fileName));
    }

    private String getLookupName(String dirName, String fileName) {
        return dirName + "~" + fileName;
    }

    public boolean isBeingReplicated(String dirName, String name) {
        return replicaNames.contains(getLookupName(dirName, name));
    }

    public void setWrapper(LocalIOWrapper wrapper) {
        this.wrapper = wrapper;
    }

    public void setLocalFileCache(LocalFileCache localFileCache) {
        this.localFileCache = localFileCache;
    }
}
