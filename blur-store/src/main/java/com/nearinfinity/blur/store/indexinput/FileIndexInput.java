package com.nearinfinity.blur.store.indexinput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.util.Constants;

public class FileIndexInput extends BufferedIndexInput {

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
