package com.nearinfinity.blur.manager.writer.lucene;

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
import java.io.IOException;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

public class SnapshotIndexReader extends FilterIndexReader {

  private final int maxDocs;
  private final int numDocs;

  public SnapshotIndexReader(IndexReader in, int maxDocs) {
    super(in);
    this.numDocs = in.numDocs();
    this.maxDocs = maxDocs;
  }

  public static IndexReader wrap(IndexReader reader, int maxDocs) throws IOException {
    IndexReader[] readers = reader.getSequentialSubReaders();
    if (readers == null) {
      return wrapInternal(reader, maxDocs);
    } else {
      IndexReader[] result = new IndexReader[readers.length];
      for (int i = 0; i < readers.length; i++) {
        result[i] = wrapInternal(readers[i], maxDocs);
      }
      return new MultiReader(result, false);
    }
  }

  private static IndexReader wrapInternal(IndexReader reader, int maxDocs) throws IOException {
    return new SnapshotIndexReader(reader, maxDocs);
  }

  @Override
  public IndexReader[] getSequentialSubReaders() {
    return null;
  }

  @Override
  public int numDocs() {
    return numDocs;
  }

  @Override
  public int maxDoc() {
    return maxDocs;
  }

  @Override
  public TermDocs termDocs() throws IOException {
    return new SnapshotTermDocs(in.termDocs(), maxDocs);
  }

  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    TermDocs termDocs = termDocs();
    termDocs.seek(term);
    return termDocs;
  }

  @Override
  public TermPositions termPositions() throws IOException {
    return new SnapshotTermPositions(in.termPositions(), maxDocs);
  }

  // public TermPositions termPositions(Term term) throws IOException {
  // ensureOpen();
  // TermPositions termPositions = termPositions();
  // termPositions.seek(term);
  // return termPositions;
  // }

  public static class SnapshotTermPositions extends FilterTermPositions {

    private final int maxDocs;

    public SnapshotTermPositions(TermPositions termPositions, int maxDocs) {
      super(termPositions);
      this.maxDocs = maxDocs;
    }

    @Override
    public boolean next() throws IOException {
      boolean next = super.next();
      if (next) {
        if (doc() >= maxDocs) {
          return false;
        }
      }
      return next;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      int read = super.read(docs, freqs);
      if (read == 0) {
        return 0;
      }
      if (doc() >= maxDocs) {
        return checkResults(docs, maxDocs);
      }
      return read;
    }
  }

  public static class SnapshotTermDocs extends FilterTermDocs {

    private final int maxDocs;

    public SnapshotTermDocs(TermDocs termDocs, int maxDocs) {
      super(termDocs);
      this.maxDocs = maxDocs;
    }

    @Override
    public boolean next() throws IOException {
      boolean next = super.next();
      if (next) {
        if (doc() >= maxDocs) {
          return false;
        }
      }
      return next;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      int read = super.read(docs, freqs);
      if (read == 0) {
        return 0;
      }
      if (doc() >= maxDocs) {
        return checkResults(docs, maxDocs);
      }
      return read;
    }
  }

  private static int checkResults(int[] docs, int maxDocs) {
    int length = docs.length;
    for (int i = 0; i < length; i++) {
      if (docs[i] >= maxDocs) {
        return i;
      }
    }
    return length;
  }
}
