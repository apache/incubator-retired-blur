package com.nearinfinity.blur.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.BufferedIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.Constants;

public class WritableHdfsDirectory extends HdfsDirectory {

    private static final String SEGMENTS_GEN = "segments.gen";

    private static final Log LOG = LogFactory.getLog(WritableHdfsDirectory.class);

    protected LocalFileCache localFileCache;
    protected String dirName;
    protected Progressable progressable;
    
    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory) throws IOException {
        this(dirName,hdfsDirPath,fileSystem,localFileCache,lockFactory,new Progressable() {
            @Override
            public void progress() {
                
            }
        });
    }

    public WritableHdfsDirectory(String dirName, Path hdfsDirPath, FileSystem fileSystem,
            LocalFileCache localFileCache, LockFactory lockFactory, Progressable progressable) throws IOException {
        super(hdfsDirPath, fileSystem);
        this.dirName = dirName;
        this.progressable = progressable;
        File segments = localFileCache.getLocalFile(dirName, SEGMENTS_GEN);
        if (segments.exists()) {
            segments.delete();
        }
        this.localFileCache = localFileCache;
        setLockFactory(lockFactory);
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        File file = localFileCache.getLocalFile(dirName, name);
        if (file.exists()) {
            file.delete();
        }
        LOG.info("Opening local file for writing [" + file.getAbsolutePath() + "]");
        return new FileIndexOutput(progressable,file);
    }

    @Override
    public void sync(String name) throws IOException {
        File file = localFileCache.getLocalFile(dirName, name);
        FSDataOutputStream outputStream = super.getOutputStream(name);
        FileInputStream inputStream = new FileInputStream(file);
        LOG.info("Syncing local file [" + file.getAbsolutePath() + "] to [" + hdfsDirPath + "]");
        byte[] buffer = new byte[BUFFER_SIZE];
        int num;
        while ((num = inputStream.read(buffer)) != -1) {
            progressable.progress();
            outputStream.write(buffer, 0, num);
        }
        outputStream.close();
        inputStream.close();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (super.fileExists(name)) {
            super.deleteFile(name);
        } else {
            localFileCache.getLocalFile(dirName, name).delete();
        }
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        if (super.fileExists(name)) {
            return true;
        }
        if (fileExistsLocally(name)) {
            return true;
        }
        return false;
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (super.fileExists(name)) {
            return super.fileLength(name);
        } else {
            return localFileCache.getLocalFile(dirName, name).length();
        }
    }

    @Override
    public long fileModified(String name) throws IOException {
        if (super.fileExists(name)) {
            return super.fileModified(name);
        } else {
            return localFileCache.getLocalFile(dirName, name).lastModified();
        }
    }

    @Override
    public String[] listAll() throws IOException {
        return super.listAll();
    }

    @Override
    public IndexInput openInput(String name, int bufferSize) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException(name);
        }
        if (!fileExistsLocally(name)) {
            return openFromHdfs(name, bufferSize);
        } else {
            return openFromLocal(name, bufferSize);
        }
    }

    public boolean fileExistsLocally(String name) {
        return localFileCache.getLocalFile(dirName, name).exists();
    }

    public FileIndexInput openFromLocal(String name, int bufferSize) throws IOException {
        if (Constants.WINDOWS) {
            return new FileIndexInput(localFileCache.getLocalFile(dirName, name), bufferSize);
        } else {
            return new FileNIOIndexInput(localFileCache.getLocalFile(dirName, name), bufferSize);
        }
    }

    public IndexInput openFromHdfs(String name, int bufferSize) throws IOException {
        return super.openInput(name, bufferSize);
    }

    public static class FileIndexInput extends BufferedIndexInput {

        protected static class Descriptor extends RandomAccessFile {
            // remember if the file is open, so that we don't try to close it
            // more than once
            protected volatile boolean isOpen;
            long position;
            final long length;

            public Descriptor(File file, String mode) throws IOException {
                super(file, mode);
                isOpen = true;
                length = length();
            }

            @Override
            public void close() throws IOException {
                if (isOpen) {
                    isOpen = false;
                    super.close();
                }
            }
        }

        protected final Descriptor file;
        boolean isClone;
        // LUCENE-1566 - maximum read length on a 32bit JVM to prevent incorrect OOM
        protected final int chunkSize = Constants.JRE_IS_64BIT ? Integer.MAX_VALUE : 100 * 1024 * 1024;

        public FileIndexInput(File path, int bufferSize) throws IOException {
            super(bufferSize);
            file = new Descriptor(path, "r");
        }

        /** IndexInput methods */
        @Override
        protected void readInternal(byte[] b, int offset, int len) throws IOException {
            synchronized (file) {
                long position = getFilePointer();
                if (position != file.position) {
                    file.seek(position);
                    file.position = position;
                }
                int total = 0;

                try {
                    do {
                        final int readLength;
                        if (total + chunkSize > len) {
                            readLength = len - total;
                        } else {
                            // LUCENE-1566 - work around JVM Bug by breaking very large reads into chunks
                            readLength = chunkSize;
                        }
                        final int i = file.read(b, offset + total, readLength);
                        if (i == -1) {
                            throw new IOException("read past EOF");
                        }
                        file.position += i;
                        total += i;
                    } while (total < len);
                } catch (OutOfMemoryError e) {
                    // propagate OOM up and add a hint for 32bit VM Users hitting the bug
                    // with a large chunk size in the fast path.
                    final OutOfMemoryError outOfMemoryError = new OutOfMemoryError(
                            "OutOfMemoryError likely caused by the Sun VM Bug described in "
                                    + "https://issues.apache.org/jira/browse/LUCENE-1566; try calling FSDirectory.setReadChunkSize "
                                    + "with a a value smaller than the current chunks size (" + chunkSize + ")");
                    outOfMemoryError.initCause(e);
                    throw outOfMemoryError;
                }
            }
        }

        @Override
        public void close() throws IOException {
            // only close the file if this is not a clone
            if (!isClone)
                file.close();
        }

        @Override
        protected void seekInternal(long position) {
        }

        @Override
        public long length() {
            return file.length;
        }

        @Override
        public Object clone() {
            FileIndexInput clone = (FileIndexInput) super.clone();
            clone.isClone = true;
            return clone;
        }

        /**
         * Method used for testing. Returns true if the underlying file descriptor is valid.
         */
        boolean isFDValid() throws IOException {
            return file.getFD().valid();
        }
    }

    public static class FileIndexOutput extends BufferedIndexOutput {

        private RandomAccessFile file = null;

        // remember if the file is open, so that we don't try to close it more than once
        private volatile boolean isOpen;

        private Progressable progressable;

        public FileIndexOutput(Progressable progressable, File path) throws IOException {
            file = new RandomAccessFile(path, "rw");
            isOpen = true;
            this.progressable = progressable;
        }

        @Override
        public void flushBuffer(byte[] b, int offset, int size) throws IOException {
            file.write(b, offset, size);
            progressable.progress();
        }

        @Override
        public void close() throws IOException {
            // only close the file if it has not been closed yet
            if (isOpen) {
                boolean success = false;
                try {
                    super.close();
                    success = true;
                } finally {
                    isOpen = false;
                    if (!success) {
                        try {
                            file.close();
                        } catch (Throwable t) {
                            // Suppress so we don't mask original exception
                        }
                    } else {
                        file.close();
                    }
                }
            }
        }

        @Override
        public void seek(long pos) throws IOException {
            super.seek(pos);
            file.seek(pos);
        }

        @Override
        public long length() throws IOException {
            return file.length();
        }

        @Override
        public void setLength(long length) throws IOException {
            file.setLength(length);
        }
    }

    public static class FileNIOIndexInput extends FileIndexInput {

        private ByteBuffer byteBuf; // wraps the buffer for NIO

        private byte[] otherBuffer;
        private ByteBuffer otherByteBuf;

        final FileChannel channel;

        public FileNIOIndexInput(File path, int bufferSize) throws IOException {
            super(path, bufferSize);
            channel = file.getChannel();
        }

        @Override
        protected void newBuffer(byte[] newBuffer) {
            super.newBuffer(newBuffer);
            byteBuf = ByteBuffer.wrap(newBuffer);
        }

        @Override
        public void close() throws IOException {
            if (!isClone && file.isOpen) {
                // Close the channel & file
                try {
                    channel.close();
                } finally {
                    file.close();
                }
            }
        }

        @Override
        protected void readInternal(byte[] b, int offset, int len) throws IOException {

            final ByteBuffer bb;

            // Determine the ByteBuffer we should use
            if (b == buffer && 0 == offset) {
                // Use our own pre-wrapped byteBuf:
                assert byteBuf != null;
                byteBuf.clear();
                byteBuf.limit(len);
                bb = byteBuf;
            } else {
                if (offset == 0) {
                    if (otherBuffer != b) {
                        // Now wrap this other buffer; with compound
                        // file, we are repeatedly called with its
                        // buffer, so we wrap it once and then re-use it
                        // on subsequent calls
                        otherBuffer = b;
                        otherByteBuf = ByteBuffer.wrap(b);
                    } else
                        otherByteBuf.clear();
                    otherByteBuf.limit(len);
                    bb = otherByteBuf;
                } else {
                    // Always wrap when offset != 0
                    bb = ByteBuffer.wrap(b, offset, len);
                }
            }

            int readOffset = bb.position();
            int readLength = bb.limit() - readOffset;
            assert readLength == len;

            long pos = getFilePointer();

            try {
                while (readLength > 0) {
                    final int limit;
                    if (readLength > chunkSize) {
                        // LUCENE-1566 - work around JVM Bug by breaking
                        // very large reads into chunks
                        limit = readOffset + chunkSize;
                    } else {
                        limit = readOffset + readLength;
                    }
                    bb.limit(limit);
                    int i = channel.read(bb, pos);
                    if (i == -1) {
                        throw new IOException("read past EOF");
                    }
                    pos += i;
                    readOffset += i;
                    readLength -= i;
                }
            } catch (OutOfMemoryError e) {
                // propagate OOM up and add a hint for 32bit VM Users hitting the bug
                // with a large chunk size in the fast path.
                final OutOfMemoryError outOfMemoryError = new OutOfMemoryError(
                        "OutOfMemoryError likely caused by the Sun VM Bug described in "
                                + "https://issues.apache.org/jira/browse/LUCENE-1566; try calling FSDirectory.setReadChunkSize "
                                + "with a a value smaller than the current chunk size (" + chunkSize + ")");
                outOfMemoryError.initCause(e);
                throw outOfMemoryError;
            }
        }
    }
}
