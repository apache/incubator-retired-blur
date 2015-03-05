package org.apache.blur.index;

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The {@link DirectoryReader} wraps a real index {@link DirectoryReader} and
 * allows for a {@link AtomicBoolean} to be checked periodically to see if the
 * thread should exit or not. The exit mechanism is by throw a
 * {@link ExitingReaderException} exception.
 */
public class ExitableReader extends FilterDirectoryReader {

  @SuppressWarnings("serial")
  public static class ExitingReaderException extends RuntimeException {

  }

  public static class ExitableSubReaderWrapper extends SubReaderWrapper {
    private final ExitObject _exitObject;

    public ExitableSubReaderWrapper(ExitObject exitObject) {
      _exitObject = exitObject;
    }

    @Override
    public AtomicReader wrap(AtomicReader reader) {
      return new ExitableFilterAtomicReader(reader, _exitObject);
    }
  }

  public static class ExitableFilterAtomicReader extends FilterAtomicReader {

    private final ExitObject _exitObject;

    public ExitableFilterAtomicReader(AtomicReader in, ExitObject exitObject) {
      super(in);
      _exitObject = exitObject;
    }

    public AtomicReader getOriginalReader() {
      return in;
    }

    @Override
    public Fields fields() throws IOException {
      Fields fields = super.fields();
      if (fields == null) {
        return null;
      }
      return new ExitableFields(fields, _exitObject);
    }

    @Override
    public Object getCoreCacheKey() {
      return in.getCoreCacheKey();
    }

    @Override
    public Object getCombinedCoreAndDeletesKey() {
      return in.getCombinedCoreAndDeletesKey();
    }
  }

  public static class ExitableFields extends Fields {

    private final ExitObject _exitObject;
    private final Fields _fields;

    public ExitableFields(Fields fields, ExitObject exitObject) {
      _fields = fields;
      _exitObject = exitObject;
    }

    @Override
    public Terms terms(String field) throws IOException {
      Terms terms = _fields.terms(field);
      if (terms == null) {
        return null;
      }
      return new ExitableTerms(terms, _exitObject);
    }

    @Override
    public Iterator<String> iterator() {
      return _fields.iterator();
    }

    @Override
    public int size() {
      return _fields.size();
    }

  }

  public static class ExitableTerms extends Terms {

    private final ExitObject _exitObject;
    private final Terms _terms;

    public ExitableTerms(Terms terms, ExitObject exitObject) {
      _terms = terms;
      _exitObject = exitObject;
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
      return new ExitableTermsEnum(_terms.intersect(compiled, startTerm), _exitObject);
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new ExitableTermsEnum(_terms.iterator(reuse), _exitObject);
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return _terms.getComparator();
    }

    @Override
    public long size() throws IOException {
      return _terms.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return _terms.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return _terms.getSumDocFreq();
    }

    @Override
    public int getDocCount() throws IOException {
      return _terms.getDocCount();
    }

    @Override
    public boolean hasOffsets() {
      return _terms.hasOffsets();
    }

    @Override
    public boolean hasPositions() {
      return _terms.hasPositions();
    }

    @Override
    public boolean hasPayloads() {
      return _terms.hasPayloads();
    }

  }

  public static class ExitableTermsEnum extends TermsEnum {

    private static final long CHECK_TIME = TimeUnit.SECONDS.toNanos(1);
    private final AtomicBoolean _running;
    private final TermsEnum _termsEnum;
    private int max = 1000;
    private long _lastCheck;
    private int count = 0;

    public ExitableTermsEnum(TermsEnum termsEnum, ExitObject exitObject) {
      _termsEnum = termsEnum;
      _running = exitObject.get();
      _lastCheck = System.nanoTime();
      checkAndThrow();
    }

    private void checkRunningState() {
      count++;
      if (count >= max) {
        long now = System.nanoTime();
        if (_lastCheck + CHECK_TIME < now) {

          checkAndThrow();
        } else {
          // diff is the actual time between last check and now
          long diff = now - _lastCheck;
          // try to re-adjust max count
          int maxShouldBe = (int) (((float) count / (float) diff) * (float) CHECK_TIME);
          max = maxShouldBe;
        }
        count = 0;
      }
    }

    private void checkAndThrow() {
      if (!_running.get()) {
        throw new ExitingReaderException();
      }
    }

    @Override
    public BytesRef next() throws IOException {
      checkRunningState();
      return _termsEnum.next();
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return _termsEnum.getComparator();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      return _termsEnum.seekCeil(text, useCache);
    }

    @Override
    public void seekExact(long ord) throws IOException {
      _termsEnum.seekExact(ord);
    }

    @Override
    public BytesRef term() throws IOException {
      return _termsEnum.term();
    }

    @Override
    public long ord() throws IOException {
      return _termsEnum.ord();
    }

    @Override
    public int docFreq() throws IOException {
      return _termsEnum.docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
      return _termsEnum.totalTermFreq();
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags) throws IOException {
      checkRunningState();
      return _termsEnum.docs(liveDocs, reuse, flags);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags)
        throws IOException {
      checkRunningState();
      return _termsEnum.docsAndPositions(liveDocs, reuse, flags);
    }

  }

  private final ExitObject _exitObject;
  private final DirectoryReader _in;

  public ExitableReader(DirectoryReader in) {
    this(in, new ExitObject());
  }

  public ExitableReader(DirectoryReader in, ExitObject exitObject) {
    super(in, new ExitableSubReaderWrapper(exitObject));
    _exitObject = exitObject;
    _in = in;
  }

  public DirectoryReader getIn() {
    return _in;
  }

  public AtomicBoolean getRunning() {
    return _exitObject.get();
  }

  public void setRunning(AtomicBoolean running) {
    _exitObject.set(running);
  }

  public void reset() {
    _exitObject.reset();
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
    return in;
  }

  @Override
  public String toString() {
    return "ExitableReader(" + in.toString() + ")";
  }
}
