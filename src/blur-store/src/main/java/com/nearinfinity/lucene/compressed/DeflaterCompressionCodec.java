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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class DeflaterCompressionCodec implements CompressionCodec {

    @Override
    public int compress(byte[] src, int length, byte[] dest) throws IOException {
        Deflater compresser = new Deflater(Deflater.BEST_SPEED);
        try {
            compresser.setInput(src, 0, length);
            compresser.finish();
            return compresser.deflate(dest);
        } finally {
            compresser.end();
        }
    }

    @Override
    public int decompress(byte[] src, int length, byte[] dest) throws IOException {
        Inflater decompresser = new Inflater();
        try {
            decompresser.setInput(src, 0, length);
            return decompresser.inflate(dest);
        } catch (DataFormatException e) {
            throw new IOException(e);
        } finally {
            decompresser.end();
        }
    }

}
