package com.nearinfinity.blur.store.replication;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.store.LocalFileCache;
import com.nearinfinity.blur.store.WritableHdfsDirectory;


public class ReplicaHdfsDirectory extends WritableHdfsDirectory {

    private static final Log LOG = LogFactory.getLog(ReplicaHdfsDirectory.class);
    private LocalIOWrapper wrapper;
    private ReplicationDaemon replicationDaemon;

    public ReplicaHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem, final LocalFileCache localFileCache, LockFactory lockFactory)
            throws IOException {
        this(dirName, hdfsDirPath, fileSystem, localFileCache, lockFactory, new LocalIOWrapper() {
            @Override
            public IndexOutput wrapOutput(IndexOutput indexOutput) {
                return indexOutput;
            }

            @Override
            public IndexInput wrapInput(IndexInput indexInput) {
                return indexInput;
            }
        });
    }

    public ReplicaHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem, final LocalFileCache localFileCache,
            LockFactory lockFactory, final LocalIOWrapper wrapper) throws IOException {
        super(dirName, hdfsDirPath, fileSystem, localFileCache, lockFactory);
        this.wrapper = wrapper;
        this.replicationDaemon = new ReplicationDaemon(dirName, this, localFileCache, wrapper);
    }

    @Override
    public IndexInput openInput(String name, int bufferSize) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        ReplicaIndexInput input = new ReplicaIndexInput(name, fileLength(name), BUFFER_SIZE, this);
        input.baseInput = new AtomicReference<IndexInput>(openFromHdfs(name, BUFFER_SIZE));
        if (fileExistsLocally(name)) {
            input.localInput = new AtomicReference<IndexInput>(wrapper.wrapInput(openFromLocal(name, BUFFER_SIZE)));
        } else {
            input.localInput = new AtomicReference<IndexInput>();
        }
        return input;
    }

    protected void readInternal(ReplicaIndexInput replicaIndexInput, byte[] b, int offset, int length)
            throws IOException {
        IndexInput baseIndexInputSource = replicaIndexInput.baseInput.get();
        IndexInput localIndexInputSource = replicaIndexInput.localInput.get();
        long filePointer = replicaIndexInput.getFilePointer();
        
        if (baseIndexInputSource == null) {
            throw new IOException("Fatal exception base indexinput is null, time to give up!");
        }
        setupBaseIndexInput(replicaIndexInput, baseIndexInputSource, filePointer);
        setupLocalIndexInput(replicaIndexInput, localIndexInputSource, filePointer);
        performRead(replicaIndexInput,filePointer,b,offset,length);
    }

    private void performRead(ReplicaIndexInput replicaIndexInput, long filePointer, byte[] b, int offset, int length) throws IOException {
        if (replicaIndexInput.localInputClone == null) {
            replicate(replicaIndexInput);
            replicaIndexInput.baseInputClone.seek(filePointer);
            replicaIndexInput.baseInputClone.readBytes(b, offset, length);
        } else {
            try {
                replicaIndexInput.localInputClone.seek(filePointer);
                replicaIndexInput.localInputClone.readBytes(b, offset, length);
            } catch (IOException e) {
                replicaIndexInput.baseInputClone.seek(filePointer);
                replicaIndexInput.baseInputClone.readBytes(b, offset, length);
                resetLocal(replicaIndexInput);
                // increment error after base read because index files might be bad
                long numberOfErrors = replicaIndexInput.errorCounter.incrementAndGet();
                LOG.error("Error reading from local [" + replicaIndexInput.name + "] at position [" + filePointer
                        + "], this file has errored out [" + numberOfErrors + "]");
            }
        }        
    }

    private void resetLocal(ReplicaIndexInput replicaIndexInput) {
        try {
            replicaIndexInput.localInput.get().close();
        } catch (IOException e) {
            LOG.error("Error trying to close file [" + replicaIndexInput.name + "] because needs to be replicated again.");
        }
        replicaIndexInput.localInput.set(null);
        replicaIndexInput.localInputRef = null;
        replicaIndexInput.localInputClone = null;
    }

    private void setupLocalIndexInput(ReplicaIndexInput replicaIndexInput, IndexInput localIndexInputSource,
            long filePointer) throws IOException {
        if (localIndexInputSource == null) {
            replicaIndexInput.localInputRef = null;
            replicaIndexInput.localInputClone = null;
        } else if (replicaIndexInput.localInputRef == null || replicaIndexInput.localInputRef != localIndexInputSource) {
            replicaIndexInput.localInputClone = (IndexInput) localIndexInputSource.clone();
            replicaIndexInput.localInputClone.seek(filePointer);
            replicaIndexInput.localInputRef = localIndexInputSource;
        }
    }

    private void setupBaseIndexInput(ReplicaIndexInput replicaIndexInput, IndexInput baseIndexInputSource,
            long filePointer) throws IOException {
        if (replicaIndexInput.baseInputRef == null || replicaIndexInput.baseInputRef != baseIndexInputSource) {
            replicaIndexInput.baseInputClone = (IndexInput) baseIndexInputSource.clone();
            replicaIndexInput.baseInputClone.seek(filePointer);
            replicaIndexInput.baseInputRef = baseIndexInputSource;
        }
    }

    private void replicate(ReplicaIndexInput replicaIndexInput) {
        replicationDaemon.replicate(replicaIndexInput);
    }

    protected void close(ReplicaIndexInput replicaIndexInput) throws IOException {
        AtomicReference<IndexInput> baseInput = replicaIndexInput.baseInput;
        AtomicReference<IndexInput> localInput = replicaIndexInput.localInput;
        String name = replicaIndexInput.name;
        synchronized (baseInput) {
            IndexInput baseIndexInput = baseInput.get();
            IndexInput localIndexInput = localInput.get();
            if (localIndexInput != null) {
                try {
                    localIndexInput.close();
                } catch (IOException e) {
                    LOG.error("Error trying to close local file [" + name + "]", e);
                }
            }
            baseIndexInput.close();
        }
    }

    public static class ReplicaIndexInput extends BufferedIndexInput {

        protected AtomicReference<IndexInput> baseInput;
        protected AtomicReference<IndexInput> localInput;
        protected AtomicLong errorCounter = new AtomicLong();

        protected IndexInput baseInputRef;
        protected IndexInput localInputRef;

        protected IndexInput baseInputClone;
        protected IndexInput localInputClone;

        private long length;
        private boolean clone;
        private ReplicaHdfsDirectory directory;
        protected String name;

        public ReplicaIndexInput(String name, long length, int bufferSize, ReplicaHdfsDirectory directory) {
            super(bufferSize);
            this.length = length;
            this.directory = directory;
            this.name = name;
        }

        @Override
        protected void readInternal(byte[] b, int offset, int length) throws IOException {
            directory.readInternal(this, b, offset, length);
        }

        @Override
        protected void seekInternal(long pos) throws IOException {

        }

        @Override
        public void close() throws IOException {
            if (!clone) {
                directory.close(this);
            }
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Object clone() {
            ReplicaIndexInput clone = (ReplicaIndexInput) super.clone();
            clone.clone = true;
            clone.baseInputRef = null;
            clone.localInputRef = null;

            clone.baseInputClone = null;
            clone.localInputClone = null;
            return clone;
        }
    }
}
