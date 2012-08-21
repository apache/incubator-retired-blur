package com.nearinfinity.blur.manager.writer.lucene;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.util.BitVector;

public class SoftDeleteIndexReader extends FilterIndexReader {

  private final boolean baseHasDeletions;
  private final BitVector deletes;
  private final int deleteCount;
  private final int numDocs;

  public SoftDeleteIndexReader(IndexReader in, BitVector deletes) {
    super(in);
    this.baseHasDeletions = in.hasDeletions();
    if (deletes == null) {
      throw new RuntimeException("No deletes, use regular indexreader.");
    }
    this.deletes = deletes;
    this.deleteCount = deletes.count();
    this.numDocs = in.numDocs() - deleteCount;
  }

  public static IndexReader wrap(IndexReader reader, Collection<Term> deleteTerms) throws IOException {
    IndexReader[] readers = reader.getSequentialSubReaders();
    if (readers == null) {
      return wrapInternal(reader, deleteTerms);
    } else {
      IndexReader[] result = new IndexReader[readers.length];
      for (int i = 0; i < readers.length; i++) {
        result[i] = wrapInternal(readers[i], deleteTerms);
      }
      return new MultiReader(result, false);
    }
  }

  private static IndexReader wrapInternal(IndexReader reader, Collection<Term> deleteTerms) throws IOException {
    BitVector deletes = getDeletes(reader, deleteTerms);
    if (deletes == null) {
      return reader;
    }
    return new SoftDeleteIndexReader(reader, deletes);
  }

  private static BitVector getDeletes(IndexReader reader, Collection<Term> deleteTerms) throws IOException {
    BitVector deletes = null;
    TermDocs termDocs = reader.termDocs();
    for (Term t : deleteTerms) {
      termDocs.seek(t);
      while (termDocs.next()) {
        if (deletes == null) {
          deletes = new BitVector(reader.maxDoc());
        }
        int doc = termDocs.doc();
        deletes.set(doc);
      }
    }
    termDocs.close();
    return deletes;
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
  public boolean isDeleted(int n) {
    if (baseHasDeletions && in.isDeleted(n)) {
      return true;
    }
    return deletes.get(n);
  }

  @Override
  public boolean hasDeletions() {
    return baseHasDeletions;
  }

  @Override
  public TermDocs termDocs() throws IOException {
    return new SoftDeleteTermDocs(in.termDocs(), deletes);
  }

  public TermDocs termDocs(Term term) throws IOException {
    ensureOpen();
    TermDocs termDocs = termDocs();
    termDocs.seek(term);
    return termDocs;
  }

  @Override
  public TermPositions termPositions() throws IOException {
    return new SoftDeleteTermPositions(in.termPositions(), deletes);
  }

//  public TermPositions termPositions(Term term) throws IOException {
//    ensureOpen();
//    TermPositions termPositions = termPositions();
//    termPositions.seek(term);
//    return termPositions;
//  }

  public static class SoftDeleteTermPositions extends FilterTermPositions {

    private BitVector deletes;

    public SoftDeleteTermPositions(TermPositions termPositions, BitVector deletes) {
      super(termPositions);
      this.deletes = deletes;
    }

    @Override
    public boolean next() throws IOException {
      while (super.next()) {
        if (!deletes.get(doc())) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      int read = super.read(docs, freqs);
      if (read == 0) {
        return 0;
      }
      int validResults = removeDeletes(docs, freqs, read, deletes);
      if (validResults == 0) {
        return read(docs, freqs);
      }
      return validResults;
    }
  }

  public static class SoftDeleteTermDocs extends FilterTermDocs {

    private BitVector deletes;

    public SoftDeleteTermDocs(TermDocs termDocs, BitVector deletes) {
      super(termDocs);
      this.deletes = deletes;
    }

    @Override
    public boolean next() throws IOException {
      while (super.next()) {
        if (!deletes.get(doc())) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {
      int read = super.read(docs, freqs);
      if (read == 0) {
        return 0;
      }
      int validResults = removeDeletes(docs, freqs, read, deletes);
      if (validResults == 0) {
        return read(docs, freqs);
      }
      return validResults;
    }
  }

  private static int removeDeletes(int[] docs, int[] freqs, int validLength, BitVector deletes) {
    int targetPosition = 0;
    for (int i = 0; i < validLength; i++) {
      int doc = docs[i];
      if (!deletes.get(doc)) {
        if (targetPosition != i) {
          docs[targetPosition] = doc;
          freqs[targetPosition] = freqs[i];
        }
        targetPosition++;
      }
    }
    return targetPosition;
  }
}
