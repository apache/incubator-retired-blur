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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileNIOIndexInput extends FileIndexInput {

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