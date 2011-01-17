package com.nearinfinity.blur.store.replication;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.nearinfinity.blur.store.Constants;
import com.nearinfinity.blur.store.WritableHdfsDirectory.FileIndexInput;
import com.nearinfinity.blur.store.WritableHdfsDirectory.FileIndexOutput;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory.ReplicaIndexInput;


public class ReplicationDaemon extends TimerTask implements Constants {
    
    private static final Log LOG = LogFactory.getLog(ReplicationDaemon.class);
    
    private Timer daemon;
    private ConcurrentMap<String, ReplicaIndexInput> replicaQueue = new ConcurrentHashMap<String, ReplicaIndexInput>();
    private LocalIOWrapper wrapper;
    private long period = TimeUnit.SECONDS.toMillis(1);

    private File tmpLocalDir;
    private ReplicaHdfsDirectory directory;

    public ReplicationDaemon(ReplicaHdfsDirectory directory, File tmpLocalDir, LocalIOWrapper wrapper) {
        this.directory = directory;
        this.tmpLocalDir = tmpLocalDir;
        this.wrapper = wrapper;
        this.daemon = new Timer("Replication-Thread", true);
        this.daemon.scheduleAtFixedRate(this, period, period);
    }
    
    @Override
    public void run() {
        try {
            byte[] buffer = new byte[1024 * 1024];
            Set<String> fileNames = new TreeSet<String>(replicaQueue.keySet());
            for (String name : fileNames) {
                System.out.println("Replicating [" + name + "]");
                ReplicaIndexInput replicaIndexInput = replicaQueue.get(name);
                IndexInput hdfsInput = directory.openFromHdfs(name, BUFFER_SIZE);
                hdfsInput.seek(0);
                File localFile = new File(tmpLocalDir, name);
                if (localFile.exists()) {
                    if (!localFile.delete()) {
                        LOG.error("Error trying to delete existing file during replication [" + localFile + "]");
                    }
                }
                IndexOutput indexOutput = wrapper.wrapOutput(new FileIndexOutput(localFile));
                copy(hdfsInput, indexOutput, buffer);
                IndexInput localInput = wrapper.wrapInput(new FileIndexInput(localFile,
                        BUFFER_SIZE));
                replicaIndexInput.localInput.set(localInput);
            }
            for (String name : fileNames) {
                replicaQueue.remove(name);
            }
        } catch (Exception e) {
            LOG.error("Error during local replication.", e);
        }
    }

    private void copy(IndexInput is, IndexOutput os, byte[] buffer) throws IOException {
        try {
            // and copy to dest directory
            long len = is.length();
            long readCount = 0;
            while (readCount < len) {
                int toRead = readCount + buffer.length > len ? (int) (len - readCount) : buffer.length;
                is.readBytes(buffer, 0, toRead);
                os.writeBytes(buffer, toRead);
                readCount += toRead;
            }
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

    public void replicate(ReplicaIndexInput replicaIndexInput) {
        replicaQueue.putIfAbsent(replicaIndexInput.name, replicaIndexInput);
    }

}
