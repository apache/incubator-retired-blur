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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.store.Constants;
import com.nearinfinity.blur.store.WritableHdfsDirectory.FileIndexOutput;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.indexinput.IndexInputFactory;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory.ReplicaIndexInput;


public class ReplicationDaemon implements Constants, Runnable {
    
    private static final String EMPTY_STRING = "";
    private static final Log LOG = LogFactory.getLog(ReplicationDaemon.class);
    
    public static class RepliaWorkUnit {
        ReplicaIndexInput replicaIndexInput;
        ReplicaHdfsDirectory directory;
        IndexInput source;
        IndexOutput output;
        File localFile;
        long startTime;
        long length;
        long readCount;
        boolean finished;
        public long lastStatus;
    }
    
    static class RepliaWorkUnitCompartor implements Comparator<RepliaWorkUnit> {
        private LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
        @Override
        public int compare(RepliaWorkUnit o1, RepliaWorkUnit o2) {
            return comparator.compare(o1.replicaIndexInput.fileName, o2.replicaIndexInput.fileName);
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
    private int numberOfBlocksToMovePerPass = 256;
    private byte[] buffer = new byte[BUFFER_SIZE];
    private LocalFileCache localFileCache;
    private Progressable progressable = new Progressable() {
        @Override
        public void progress() {
            
        }
    };
    private PriorityBlockingQueue<RepliaWorkUnit> replicaQueue = new PriorityBlockingQueue<RepliaWorkUnit>(1024, new RepliaWorkUnitCompartor());
    private Collection<String> replicaNames = Collections.synchronizedCollection(new HashSet<String>());
    private LuceneIndexFileComparator comparator = new LuceneIndexFileComparator();
    private volatile boolean closed;
    private IndexInputFactory indexInputFactory = new IndexInputFactory() {
        @Override
        public void replicationComplete(RepliaWorkUnit workUnit, LocalIOWrapper wrapper, int bufferSize) throws IOException {
            IndexInput localInput = wrapper.wrapInput(workUnit.directory.openFromLocal(workUnit.replicaIndexInput.fileName, BUFFER_SIZE));
            workUnit.replicaIndexInput.localInput.set(localInput);
        }
    };
    private ReplicationStrategy replicationStrategy = new ReplicationStrategy() {
        @Override
        public boolean replicateLocally(String table, String name) {
            return true;
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
            if (workUnit == null) {
                workUnit = replicaQueue.take();
            } else {
                //checking to see if another higher priority file is on the queue
                workUnit = checkFilePriority(workUnit);
            }
            setupWorkUnit(workUnit);
            copy(workUnit);
            if (finishWorkUnit(workUnit)) {
                workUnit = null;
            }
        }
    }

    private boolean finishWorkUnit(RepliaWorkUnit workUnit) throws IOException {
        if (workUnit.finished) {
            close(workUnit.source,workUnit.output);
            ReplicaIndexInput replicaIndexInput = workUnit.replicaIndexInput;
            replicaNames.remove(getLookupName(replicaIndexInput.dirName,replicaIndexInput.fileName));
            indexInputFactory.replicationComplete(workUnit, wrapper, BUFFER_SIZE);
            return true;
        }
        return false;
    }

    private void setupWorkUnit(RepliaWorkUnit workUnit) throws IOException {
        ReplicaHdfsDirectory directory = workUnit.directory;
        ReplicaIndexInput replicaIndexInput = workUnit.replicaIndexInput;
        String fileName = replicaIndexInput.fileName;
        String dirName = replicaIndexInput.dirName;
        if (workUnit.source == null) {
            LOG.info("Setup for replicating [{0}] to local machine.",replicaIndexInput);
            workUnit.source = directory.openFromHdfs(fileName, BUFFER_SIZE);
            workUnit.source.seek(0);
            workUnit.localFile = localFileCache.getLocalFile(dirName, fileName);
            if (workUnit.localFile.exists()) {
                if (!workUnit.localFile.delete()) {
                    LOG.fatal("Error trying to delete existing file during replication [{0}]", workUnit.localFile);
                } else {
                    workUnit.localFile = localFileCache.getLocalFile(dirName, fileName);
                }
            }
            workUnit.output = wrapper.wrapOutput(new FileIndexOutput(progressable,workUnit.localFile));
            workUnit.startTime = System.currentTimeMillis();
            workUnit.length = workUnit.source.length();
        }
    }

    private RepliaWorkUnit checkFilePriority(RepliaWorkUnit workUnit) throws InterruptedException {
        RepliaWorkUnit next = replicaQueue.peek();
        if (keepWorkingOnCurrent(workUnit,next)) {
            return workUnit;
        } else {
            replicaQueue.put(workUnit);
            return replicaQueue.take();
        }
    }
    
    private boolean keepWorkingOnCurrent(RepliaWorkUnit current, RepliaWorkUnit next) {
        if (sameTypeOfFile(current,next)) {
            return true;
        }
        String currentFileName = current.replicaIndexInput.fileName;
        String nextFileName = next.replicaIndexInput.fileName;
        if (comparator.compare(currentFileName, nextFileName) > 0) {
            LOG.info("File [" + nextFileName + "] is higher priority than [" + currentFileName + "]");
            return false;
        }
        return true;
    }

    private boolean sameTypeOfFile(RepliaWorkUnit current, RepliaWorkUnit next) {
        String currentFileName = current.replicaIndexInput.fileName;
        String nextFileName = next.replicaIndexInput.fileName;
        if (getExt(currentFileName).equals(getExt(nextFileName))) {
            return true;
        }
        return false;
    }

    private String getExt(String fileName) {
        int index = fileName.indexOf('.');
        if (index < 0) {
            return EMPTY_STRING;
        }
        return fileName.substring(index + 1);
    }

    private void close(IndexInput source, IndexOutput output) throws IOException {
        try {
            if (output != null) {
                output.close();
            }
        } finally {
            if (source != null) {
                source.close();
            }
        }
    }

    private void copy(RepliaWorkUnit repliaWorkUnit) throws IOException {
        if (isFinished(repliaWorkUnit)) {
            repliaWorkUnit.finished = true;
            logStatus(repliaWorkUnit);
            return;
        }
        for (int i = 0; i < numberOfBlocksToMovePerPass; i++) {
            int toRead = repliaWorkUnit.readCount + buffer.length > repliaWorkUnit.length ? (int) (repliaWorkUnit.length - repliaWorkUnit.readCount) : buffer.length;
            repliaWorkUnit.source.readBytes(buffer, 0, toRead);
            repliaWorkUnit.output.writeBytes(buffer, toRead);
            repliaWorkUnit.readCount += toRead;
            if (isFinished(repliaWorkUnit)) {
                repliaWorkUnit.finished = true;
                logStatus(repliaWorkUnit);
                return;
            }
        }
        logStatus(repliaWorkUnit);
        throttleCopy(repliaWorkUnit);
    }

    private void throttleCopy(RepliaWorkUnit repliaWorkUnit) {
        try {
            Thread.sleep(TimeUnit.MILLISECONDS.toMillis(5));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isFinished(RepliaWorkUnit repliaWorkUnit) {
        if (repliaWorkUnit.readCount < repliaWorkUnit.length) {
            return false;
        }
        return true;
    }

    private void logStatus(RepliaWorkUnit workUnit) {
        long now = System.currentTimeMillis();
        if (workUnit.lastStatus + TimeUnit.SECONDS.toMillis(10) >= now) {
            return;
        }
        workUnit.lastStatus = now;
        long totalTime = now - workUnit.startTime;
        double seconds = (totalTime == 0 ? 1 : totalTime) / 1000.0;
        double totalMBytes = workUnit.length / (1024.0 * 1024.0);
        double currentMBytes = workUnit.readCount / (1024.0 * 1024.0);
        
        int percentComplete = (int) Math.round(currentMBytes / totalMBytes * 100.0);
        double mByteRate = currentMBytes / seconds;

        if (workUnit.finished) {
            LOG.info("Replication Complete of [" + workUnit.replicaIndexInput + "] at a rate of [" + mByteRate + "] MB/s");
        } else {
            LOG.info("Replication of [" + workUnit.replicaIndexInput + "] is [" + percentComplete + "%] complete, at a rate of [" + mByteRate + "] MB/s");
        }
    }
    
    public void replicate(ReplicaHdfsDirectory directory, ReplicaIndexInput replicaIndexInput) {
        if (!replicationStrategy.replicateLocally(replicaIndexInput.tableName, replicaIndexInput.fileName)) {
            return;
        }
        if (isBeingReplicated(replicaIndexInput.dirName,replicaIndexInput.fileName)) {
            return;
        }
        RepliaWorkUnit unit = new RepliaWorkUnit();
        unit.replicaIndexInput = replicaIndexInput;
        unit.directory = directory;
        replicaQueue.add(unit);
        replicaNames.add(getLookupName(replicaIndexInput.dirName,replicaIndexInput.fileName));
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

    public void setProgressable(Progressable progressable) {
        this.progressable = progressable;
    }

}
