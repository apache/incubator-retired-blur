package com.nearinfinity.blur.store.replication;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Timer;
import java.util.TimerTask;
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
import com.nearinfinity.blur.store.indexinput.FileIndexInput;
import com.nearinfinity.blur.store.indexinput.IndexInputFactory;
//import com.nearinfinity.blur.store.indexinput.FileIndexInput;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory.ReplicaIndexInput;


public class ReplicationDaemon extends TimerTask implements Constants {
    
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
    
    private Timer daemon;
    private LocalIOWrapper wrapper;
    private long period = TimeUnit.SECONDS.toMillis(1);
    private int numberOfBlocksToMovePerPass = 256;

    private LocalFileCache localFileCache;
    private Progressable progressable;
    private PriorityBlockingQueue<RepliaWorkUnit> replicaQueue = new PriorityBlockingQueue<RepliaWorkUnit>(1024, new RepliaWorkUnitCompartor());
    private IndexInputFactory indexInputFactory = new IndexInputFactory() {
        @Override
        public void replicationComplete(RepliaWorkUnit workUnit, LocalIOWrapper wrapper, int bufferSize) throws IOException {
          IndexInput localInput = wrapper.wrapInput(new FileIndexInput(workUnit.localFile, BUFFER_SIZE));
          workUnit.replicaIndexInput.localInput.set(localInput);
        }
    };

    private volatile String beingProcessedName;

    public ReplicationDaemon(LocalFileCache localFileCache, LocalIOWrapper wrapper, Progressable progressable) {
        this.localFileCache = localFileCache;
        this.wrapper = wrapper;
        this.progressable = progressable;
        this.daemon = new Timer("Replication-Thread", true);
        this.daemon.scheduleAtFixedRate(this, period, period);
    }
    
    public ReplicationDaemon(LocalFileCache localFileCache) {
        this(localFileCache,new LocalIOWrapper() {
            @Override
            public IndexInput wrapInput(IndexInput fileIndexInput) {
                return fileIndexInput;
            }
            @Override
            public IndexOutput wrapOutput(IndexOutput fileIndexOutput) {
                return fileIndexOutput;
            }
        }, new Progressable() {
            @Override
            public void progress() {
            }
        });
    }

    @Override
    public void run() {
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            while (!replicaQueue.isEmpty()) {
                RepliaWorkUnit workUnit = replicaQueue.take();
                ReplicaHdfsDirectory directory = workUnit.directory;
                ReplicaIndexInput replicaIndexInput = workUnit.replicaIndexInput;
                String fileName = replicaIndexInput.fileName;
                String dirName = replicaIndexInput.dirName;
                beingProcessedName = fileName;
                if (workUnit.source == null) {
                    LOG.info("Setup for replicating [{0}] to local machine.",replicaIndexInput);
                    workUnit.source = directory.openFromHdfs(fileName, BUFFER_SIZE);
                    workUnit.source.seek(0);
                    workUnit.localFile = localFileCache.getLocalFile(dirName, fileName);
                    if (workUnit.localFile.exists()) {
                        if (!workUnit.localFile.delete()) {
                            LOG.error("Error trying to delete existing file during replication [{0}]", workUnit.localFile);
                        }
                    }
                    workUnit.output = wrapper.wrapOutput(new FileIndexOutput(progressable,workUnit.localFile));
                    workUnit.startTime = System.currentTimeMillis();
                    workUnit.length = workUnit.source.length();
                }
                copy(workUnit, buffer);
                if (!workUnit.finished) {
                    replicaQueue.put(workUnit);
                } else {
                    close(workUnit.source,workUnit.output);
                    indexInputFactory.replicationComplete(workUnit, wrapper, BUFFER_SIZE);
                }
                beingProcessedName = null;
            }
        } catch (Exception e) {
            LOG.error("Error during local replication.", e);
        }
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

    private void copy(RepliaWorkUnit repliaWorkUnit, byte[] buffer) throws IOException {
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
            LOG.info("Replication of [" + workUnit.replicaIndexInput + "] is [" + percentComplete + 
                    "%] complete, at a rate of [" + mByteRate + "] MB/s");
        }
    }

    public void replicate(ReplicaHdfsDirectory directory, ReplicaIndexInput replicaIndexInput) {
        RepliaWorkUnit unit = new RepliaWorkUnit();
        unit.replicaIndexInput = replicaIndexInput;
        unit.directory = directory;
        replicaQueue.add(unit);
    }

    public boolean isBeingReplicated(String name) {
        return name.equals(beingProcessedName);
    }

}
