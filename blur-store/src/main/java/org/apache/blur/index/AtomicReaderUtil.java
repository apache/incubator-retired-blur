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
package org.apache.blur.index;

import java.io.IOException;

import lucene.security.index.SecureAtomicReader;

import org.apache.blur.index.ExitableReader.ExitableFilterAtomicReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;

public class AtomicReaderUtil {

  /**
   * WARNING!!! This method can bypass access control. Use only when you are
   * sure you know what you are doing!
   * 
   * @param indexReader
   * @return
   * @throws IOException
   */
  public static SegmentReader getSegmentReader(IndexReader indexReader) throws IOException {
    if (indexReader instanceof SegmentReader) {
      return (SegmentReader) indexReader;
    }
    if (indexReader instanceof ExitableFilterAtomicReader) {
      ExitableFilterAtomicReader exitableFilterAtomicReader = (ExitableFilterAtomicReader) indexReader;
      AtomicReader originalReader = exitableFilterAtomicReader.getOriginalReader();
      return getSegmentReader(originalReader);
    }
    if (indexReader instanceof SecureAtomicReader) {
      SecureAtomicReader secureAtomicReader = (SecureAtomicReader) indexReader;
      AtomicReader originalReader = secureAtomicReader.getOriginalReader();
      return getSegmentReader(originalReader);
    }
    throw new IOException("SegmentReader could not be found.");
  }
}
