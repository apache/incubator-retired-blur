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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.util.Constants;

public class FileIndexInput extends BufferedIndexInput {

    public static class Descriptor extends RandomAccessFile {
        // remember if the file is open, so that we don't try to close it
        // more than once
        protected volatile boolean isOpen;
        long position;
        final long length;
        public static AtomicInteger openCount = new AtomicInteger();

        public Descriptor(File file, String mode) throws IOException {
            super(file, mode);
            openCount.incrementAndGet();
            isOpen = true;
            length = length();
        }

        @Override
        public void close() throws IOException {
            if (isOpen) {
                isOpen = false;
                super.close();
                openCount.decrementAndGet();
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
