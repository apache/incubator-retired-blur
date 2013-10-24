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
package org.apache.blur.filter;

import java.io.Closeable;
import java.io.IOException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class IndexFileBitSet extends DocIdSet implements Closeable {

  private static final Log LOG = LogFactory.getLog(IndexFileBitSet.class);

  public static final String EXTENSION = ".filter";

  private final String _id;
  private final String _segmentName;
  private final Directory _directory;
  private final int _numBits;
  private IndexInput _indexInput;

  public IndexFileBitSet(int numBits, String id, String segmentName, Directory directory) {
    _id = id;
    _segmentName = segmentName;
    _directory = directory;
    _numBits = numBits;
  }

  @Override
  public DocIdSetIterator iterator() throws IOException {
    return new IndexFileBitSetIterator(_indexInput.clone());
  }

  public boolean exists() throws IOException {
    boolean fileExists = _directory.fileExists(getFileName());
    if (fileExists) {
      int words = (_numBits / 64) + 1;
      int correctLength = words * 8;
      long length = _indexInput.length();
      if (correctLength == length) {
        return true;
      }
    }
    return false;
  }

  private String getFileName() {
    return _segmentName + "_" + _id + EXTENSION;
  }

  public void load() throws IOException {
    String fileName = getFileName();
    _indexInput = _directory.openInput(fileName, IOContext.READ);
    int words = (_numBits / 64) + 1;
    int correctLength = words * 8;
    long length = _indexInput.length();
    if (correctLength != length) {
      throw new IOException("File [" + fileName + "] with length [" + length + "] does not match correct length of ["
          + correctLength + "]");
    }
  }

  public void create(DocIdSetIterator it) throws IOException {
    String fileName = getFileName();
    if (_directory.fileExists(getFileName())) {
      LOG.warn("Filter [{0}] in directory [{1}] being recreated due to incorrect size.", fileName, _directory);
      _directory.deleteFile(fileName);
    }
    IndexOutput output = _directory.createOutput(fileName, IOContext.READ);
    int index;
    int currentWordNum = 0;
    long wordValue = 0;
    while ((index = it.nextDoc()) < _numBits) {
      int wordNum = index >> 6; // div 64
      if (currentWordNum > wordNum) {
        throw new IOException("We got a problem here!");
      }
      while (currentWordNum < wordNum) {
        output.writeLong(wordValue);
        currentWordNum++;
        wordValue = 0;
      }
      int bit = index & 0x3f; // mod 64
      long bitmask = 1L << bit;
      wordValue |= bitmask;
    }
    if (_numBits > 0) {
      int totalWords = (_numBits / 64) + 1;
      while (currentWordNum < totalWords) {
        output.writeLong(wordValue);
        currentWordNum++;
        wordValue = 0;
      }
    }
    output.close();
  }

  @Override
  public void close() throws IOException {
    _indexInput.close();
  }
}
