package com.nearinfinity.blur.store.replication;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.blur.store.Constants;
import com.nearinfinity.blur.store.WritableHdfsDirectory.FileIndexInput;
import com.nearinfinity.blur.store.WritableHdfsDirectory.FileIndexOutput;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory.ReplicaIndexInput;


public class ReplicationDaemon extends TimerTask implements Constants {
    
    private static final Log LOG = LogFactory.getLog(ReplicationDaemon.class);
    
    static class RepliaWorkUnit {
        ReplicaIndexInput input;
        ReplicaHdfsDirectory directory;
    }
    
    private Timer daemon;
    private LocalIOWrapper wrapper;
    private long period = TimeUnit.SECONDS.toMillis(1);

    private LocalFileCache localFileCache;
    private Progressable progressable;
    private BlockingQueue<RepliaWorkUnit> replicaQueue = new LinkedBlockingQueue<RepliaWorkUnit>();

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
                ReplicaIndexInput replicaIndexInput = workUnit.input;
                String fileName = replicaIndexInput.fileName;
                String dirName = replicaIndexInput.dirName;
                
                beingProcessedName = fileName;
                LOG.info("Replicating to local machine [" + replicaIndexInput + "]");
                IndexInput hdfsInput = directory.openFromHdfs(fileName, BUFFER_SIZE);
                hdfsInput.seek(0);
                
                File localFile = localFileCache.getLocalFile(dirName, fileName);
                if (localFile.exists()) {
                    if (!localFile.delete()) {
                        LOG.error("Error trying to delete existing file during replication [" + localFile + "]");
                    }
                }
                IndexOutput indexOutput = wrapper.wrapOutput(new FileIndexOutput(progressable,localFile));
                copy(replicaIndexInput, hdfsInput, indexOutput, buffer);
                IndexInput localInput = wrapper.wrapInput(new FileIndexInput(localFile, BUFFER_SIZE));
                replicaIndexInput.localInput.set(localInput);
                beingProcessedName = null;
            }
        } catch (Exception e) {
            LOG.error("Error during local replication.", e);
        }
    }

    private void copy(ReplicaIndexInput replicaIndexInput, IndexInput is, IndexOutput os, byte[] buffer) throws IOException {
        try {
            long start = System.currentTimeMillis();
            long s = start;
            // and copy to dest directory
            long len = is.length();
            long readCount = 0;
            while (readCount < len) {
                if (s + 1000 < System.currentTimeMillis()) {
                    logStatus(readCount, len, start, replicaIndexInput);
                    s = System.currentTimeMillis();
                }
                int toRead = readCount + buffer.length > len ? (int) (len - readCount) : buffer.length;
                is.readBytes(buffer, 0, toRead);
                os.writeBytes(buffer, toRead);
                readCount += toRead;
            }
            logStatus(readCount, len, start, replicaIndexInput);
        } finally {
            // graceful cleanup
            try {
                if (os != null) {
                    os.close();
                }
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }
    }

    private void logStatus(long currentPosition, long totalLength, long startTime, ReplicaIndexInput replicaIndexInput) {
        long now = System.currentTimeMillis();
        double seconds = (now - startTime) / 1000.0;
        double totalMBytes = totalLength / (1024.0 * 1024.0);
        double currentMBytes = currentPosition / (1024.0 * 1024.0);
        
        int percentComplete = (int) Math.round(currentMBytes / totalMBytes * 100.0);
        double mByteRate = currentMBytes / seconds;
        
        LOG.info("Replication of [" + replicaIndexInput + "] is [" + percentComplete + 
        		"%] complete, at a rate of [" + mByteRate + 
        		"] MB/s");
        
    }

    public void replicate(ReplicaHdfsDirectory directory, ReplicaIndexInput replicaIndexInput) {
        RepliaWorkUnit unit = new RepliaWorkUnit();
        unit.input = replicaIndexInput;
        unit.directory = directory;
        replicaQueue.add(unit);
    }

    public boolean isBeingReplicated(String name) {
        return name.equals(beingProcessedName);
    }

}
