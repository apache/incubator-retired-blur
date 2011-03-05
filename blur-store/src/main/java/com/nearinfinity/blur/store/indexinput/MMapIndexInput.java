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

package com.nearinfinity.blur.store.indexinput;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Constants;

public class MMapIndexInput extends IndexInput {
    
    /**
     * <code>true</code>, if this platform supports unmapping mmapped files.
     */
    public static final boolean UNMAP_SUPPORTED;
    
    static {
        boolean v;
        try {
            Class.forName("sun.misc.Cleaner");
            Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
            v = true;
        } catch (Exception e) {
            v = false;
        }
        UNMAP_SUPPORTED = v;
    }

    private int maxBBuf = Constants.JRE_IS_64BIT ? Integer.MAX_VALUE : (256 * 1024 * 1024);
    private IndexInput delegate;

    public MMapIndexInput(File file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        try {
            delegate = (raf.length() <= (long) maxBBuf) ? (IndexInput) new SingleMMapIndexInput(raf)
                    : (IndexInput) new MultiMMapIndexInput(raf, maxBBuf);
        } finally {
            raf.close();
        }
    }

    /**
     * Try to unmap the buffer, this method silently fails if no support for that in the JVM. On Windows, this leads to
     * the fact, that mmapped files cannot be modified or deleted.
     */
    final static void cleanMapping(final ByteBuffer buffer) throws IOException {
        if (UNMAP_SUPPORTED) {
            try {
                AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        final Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        final Object cleaner = getCleanerMethod.invoke(buffer);
                        if (cleaner != null) {
                            cleaner.getClass().getMethod("clean").invoke(cleaner);
                        }
                        return null;
                    }
                });
            } catch (PrivilegedActionException e) {
                final IOException ioe = new IOException("unable to unmap the mapped buffer");
                ioe.initCause(e.getCause());
                throw ioe;
            }
        }
    }

    private static class SingleMMapIndexInput extends IndexInput {

        private ByteBuffer buffer;
        private final long length;
        private boolean isClone = false;

        private SingleMMapIndexInput(RandomAccessFile raf) throws IOException {
            this.length = raf.length();
            this.buffer = raf.getChannel().map(MapMode.READ_ONLY, 0, length);
        }

        @Override
        public byte readByte() throws IOException {
            try {
                return buffer.get();
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            try {
                buffer.get(b, offset, len);
            } catch (BufferUnderflowException e) {
                throw new IOException("read past EOF");
            }
        }

        @Override
        public long getFilePointer() {
            return buffer.position();
        }

        @Override
        public void seek(long pos) throws IOException {
            buffer.position((int) pos);
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Object clone() {
            SingleMMapIndexInput clone = (SingleMMapIndexInput) super.clone();
            clone.isClone = true;
            clone.buffer = buffer.duplicate();
            return clone;
        }

        @Override
        public void close() throws IOException {
            if (isClone || buffer == null)
                return;
            // unmap the buffer (if enabled) and at least unset it for GC
            try {
                cleanMapping(buffer);
            } finally {
                buffer = null;
            }
        }
    }

    // Because Java's ByteBuffer uses an int to address the
    // values, it's necessary to access a file >
    // Integer.MAX_VALUE in size using multiple byte buffers.
    private static class MultiMMapIndexInput extends IndexInput {

        private ByteBuffer[] buffers;
        private int[] bufSizes; // keep here, ByteBuffer.size() method is optional

        private final long length;

        private int curBufIndex;
        private final int maxBufSize;

        private ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]
        private int curAvail; // redundant for speed: (bufSizes[curBufIndex] - curBuf.position())

        private boolean isClone = false;

        public MultiMMapIndexInput(RandomAccessFile raf, int maxBufSize) throws IOException {
            this.length = raf.length();
            this.maxBufSize = maxBufSize;

            if (maxBufSize <= 0)
                throw new IllegalArgumentException("Non positive maxBufSize: " + maxBufSize);

            if ((length / maxBufSize) > Integer.MAX_VALUE)
                throw new IllegalArgumentException("RandomAccessFile too big for maximum buffer size: "
                        + raf.toString());

            int nrBuffers = (int) (length / maxBufSize);
            if (((long) nrBuffers * maxBufSize) < length)
                nrBuffers++;

            this.buffers = new ByteBuffer[nrBuffers];
            this.bufSizes = new int[nrBuffers];

            long bufferStart = 0;
            FileChannel rafc = raf.getChannel();
            for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
                int bufSize = (length > (bufferStart + maxBufSize)) ? maxBufSize : (int) (length - bufferStart);
                this.buffers[bufNr] = rafc.map(MapMode.READ_ONLY, bufferStart, bufSize);
                this.bufSizes[bufNr] = bufSize;
                bufferStart += bufSize;
            }
            seek(0L);
        }

        @Override
        public byte readByte() throws IOException {
            // Performance might be improved by reading ahead into an array of
            // e.g. 128 bytes and readByte() from there.
            if (curAvail == 0) {
                curBufIndex++;
                if (curBufIndex >= buffers.length)
                    throw new IOException("read past EOF");
                curBuf = buffers[curBufIndex];
                curBuf.position(0);
                curAvail = bufSizes[curBufIndex];
            }
            curAvail--;
            return curBuf.get();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            while (len > curAvail) {
                curBuf.get(b, offset, curAvail);
                len -= curAvail;
                offset += curAvail;
                curBufIndex++;
                if (curBufIndex >= buffers.length)
                    throw new IOException("read past EOF");
                curBuf = buffers[curBufIndex];
                curBuf.position(0);
                curAvail = bufSizes[curBufIndex];
            }
            curBuf.get(b, offset, len);
            curAvail -= len;
        }

        @Override
        public long getFilePointer() {
            return ((long) curBufIndex * maxBufSize) + curBuf.position();
        }

        @Override
        public void seek(long pos) throws IOException {
            curBufIndex = (int) (pos / maxBufSize);
            curBuf = buffers[curBufIndex];
            int bufOffset = (int) (pos - ((long) curBufIndex * maxBufSize));
            curBuf.position(bufOffset);
            curAvail = bufSizes[curBufIndex] - bufOffset;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Object clone() {
            MultiMMapIndexInput clone = (MultiMMapIndexInput) super.clone();
            clone.isClone = true;
            clone.buffers = new ByteBuffer[buffers.length];
            // No need to clone bufSizes.
            // Since most clones will use only one buffer, duplicate() could also be
            // done lazy in clones, e.g. when adapting curBuf.
            for (int bufNr = 0; bufNr < buffers.length; bufNr++) {
                clone.buffers[bufNr] = buffers[bufNr].duplicate();
            }
            try {
                clone.seek(getFilePointer());
            } catch (IOException ioe) {
                RuntimeException newException = new RuntimeException(ioe);
                newException.initCause(ioe);
                throw newException;
            }
            ;
            return clone;
        }

        @Override
        public void close() throws IOException {
            if (isClone || buffers == null)
                return;
            try {
                for (int bufNr = 0; bufNr < buffers.length; bufNr++) {
                    // unmap the buffer (if enabled) and at least unset it for GC
                    try {
                        cleanMapping(buffers[bufNr]);
                    } finally {
                        buffers[bufNr] = null;
                    }
                }
            } finally {
                buffers = null;
            }
        }
    }

    public Object clone() {
        return delegate.clone();
    }

    public void close() throws IOException {
        delegate.close();
    }

    public long getFilePointer() {
        return delegate.getFilePointer();
    }

    public long length() {
        return delegate.length();
    }

    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
        delegate.readBytes(b, offset, len);
    }

    public void seek(long pos) throws IOException {
        delegate.seek(pos);
    }
}
