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

package com.nearinfinity.lucene.compressed;

import java.io.IOException;

public interface CompressionCodec {
    
    /**
     * Compresses the src byte buffer into the dest byte buffer and return the length of the compressed data.
     * NOTE: The dest byte buffer will always be 2 times the size of the src.
     * @param src the source byte array (original data).
     * @param length the length of the source byte array (original data length).
     * @param dest the destination of the compressed data (decompressed data).
     * @return the length of the compressed data.
     * @throws IOException
     */
    int compress(byte[] src, int length, byte[] dest) throws IOException;

    /**
     * Decompresses the src byte buffer into the dest buffer and returns the length of the decompressed data.
     * @param src the source byte array (compressed data buffer).
     * @param length the length of the compressed data. 
     * @param dest the destination of the decompressed data (original data).
     * @return the length of the original data.
     * @throws IOException
     */
    int decompress(byte[] src, int length, byte[] dest) throws IOException;
}
