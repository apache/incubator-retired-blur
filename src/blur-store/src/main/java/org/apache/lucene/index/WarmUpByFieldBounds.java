package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class WarmUpByFieldBounds {

  public static void main(String[] args) throws CorruptIndexException, IOException {
    Directory dir = getDir();
    IndexReader reader = IndexReader.open(dir);
    AtomicBoolean isClosed = new AtomicBoolean(false);
    WarmUpByFieldBounds warmUpByFieldBounds = new WarmUpByFieldBounds();
    WarmUpByFieldBoundsStatus status = new WarmUpByFieldBoundsStatus() {
      @Override
      public void complete(String name, Term start, Term end, long startPosition, long endPosition, long totalBytesRead, long nanoTime, AtomicBoolean isClosed) {
        // System.out.println(name + " " + start + " " + end + " " +
        // startPosition + " " + endPosition + " " + totalBytesRead + " " +
        // nanoTime + " " + isClosed);
        
        double bytesPerNano = totalBytesRead / (double) nanoTime;
        double mBytesPerNano = bytesPerNano / 1024 / 1024;
        double mBytesPerSecond = mBytesPerNano * 1000000000.0;
        if (totalBytesRead > 0) {
          System.out.println("Precached field [" + start.field() + "] in file [" + name + "], " + totalBytesRead + " bytes cached at [" + mBytesPerSecond + " MB/s]");
        }
      }
    };
    warmUpByFieldBounds.warmUpByField(isClosed, new Term("f1"), reader, status);
    warmUpByFieldBounds.warmUpByField(isClosed, new Term("f0"), reader, status);
    warmUpByFieldBounds.warmUpByField(isClosed, new Term("f9"), reader, status);
    warmUpByFieldBounds.warmUpByField(isClosed, new Term("f"), reader, status);
  }

  private static Directory getDir() throws CorruptIndexException, LockObtainFailedException, IOException {
    RAMDirectory dir = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer());
    TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
    mergePolicy.setUseCompoundFile(false);
    IndexWriter writer = new IndexWriter(dir, conf);
    for (int i = 0; i < 100000; i++) {
      writer.addDocument(getDoc());
    }
    writer.close();
    return dir;
  }

  private static Document getDoc() {
    Document document = new Document();
    for (int i = 0; i < 10; i++) {
      document.add(new Field("f" + i, UUID.randomUUID().toString(), Store.YES, Index.ANALYZED));
    }
    return document;
  }

  private static final Log LOG = LogFactory.getLog(WarmUpByFieldBounds.class);

  public void warmUpByField(AtomicBoolean isClosed, Term term, IndexReader reader, WarmUpByFieldBoundsStatus status) throws IOException {
    Collection<String> fieldNames = reader.getFieldNames(FieldOption.INDEXED);
    List<String> fields = new ArrayList<String>(fieldNames);
    Collections.sort(fields);
    int index = fields.indexOf(term.field);
    if (index < fields.size() - 1) {
      warmUpByTermRange(isClosed, term, new Term(fields.get(index + 1)), reader, status);
    } else {
      warmUpByTermRange(isClosed, term, null, reader, status);
    }
  }

  public void warmUpByTermRange(AtomicBoolean isClosed, Term start, Term end, IndexReader reader, WarmUpByFieldBoundsStatus status) throws IOException {
    if (reader instanceof SegmentReader) {
      warmUpByTermRangeSegmentReader(isClosed, start, end, (SegmentReader) reader, status);
      return;
    }
    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders == null) {
      throw new RuntimeException("Reader is not supported [" + reader.getClass() + "] [" + reader + "]");
    }
    for (int i = 0; i < subReaders.length; i++) {
      warmUpByTermRange(isClosed, start, end, subReaders[i], status);
    }
  }

  private static void warmUpByTermRangeSegmentReader(AtomicBoolean isClosed, Term start, Term end, SegmentReader reader, WarmUpByFieldBoundsStatus status) throws IOException {
    SegmentCoreReaders core = reader.core;
    TermInfosReader termsReader = core.getTermsReader();
    Directory directory = reader.directory();
    String segmentName = reader.getSegmentName();
    IndexInput tis = null;
    IndexInput frq = null;
    IndexInput prx = null;
    try {
      String nameTis = segmentName + ".tis";
      String nameFrq = segmentName + ".frq";
      String namePrx = segmentName + ".prx";
      tis = directory.openInput(nameTis);
      frq = directory.openInput(nameFrq);
      prx = directory.openInput(namePrx);

      long startTermPointer = 0;
      long endTermPointer = tis.length();
      long startFreqPointer = 0;
      long endFreqPointer = frq.length();
      long startProxPointer = 0;
      long endProxPointer = prx.length();
      if (start != null) {
        Term realStartTerm = getFirstTerm(start, reader);
        if (realStartTerm == null) {
          return;
        }
        TermInfo startTermInfo = termsReader.get(realStartTerm);
        startTermPointer = termsReader.getPosition(realStartTerm);
        startFreqPointer = startTermInfo.freqPointer;
        startProxPointer = startTermInfo.proxPointer;
      }

      if (end != null) {
        Term realEndTerm = getFirstTerm(end, reader);
        if (realEndTerm == null) {
          return;
        }
        TermInfo endTermInfo = termsReader.get(realEndTerm);
        endTermPointer = termsReader.getPosition(realEndTerm);
        endFreqPointer = endTermInfo.freqPointer;
        endProxPointer = endTermInfo.proxPointer;
      }
      readFile(isClosed, tis, startTermPointer, endTermPointer, status, start, end, nameTis);
      readFile(isClosed, frq, startFreqPointer, endFreqPointer, status, start, end, nameFrq);
      readFile(isClosed, prx, startProxPointer, endProxPointer, status, start, end, namePrx);
    } finally {
      close(tis);
      close(frq);
      close(prx);
    }
  }

  private static Term getFirstTerm(Term t, SegmentReader reader) throws IOException {
    TermEnum terms = reader.terms(t);
    try {
      if (terms.next()) {
        return terms.term();
      }
      return null;
    } finally {
      terms.close();
    }
  }

  private static void close(IndexInput input) {
    try {
      if (input != null) {
        input.close();
      }
    } catch (IOException e) {
      LOG.error("Error while trying to close file [" + input + "]", e);
    }
  }

  private static void readFile(AtomicBoolean isClosed, IndexInput input, long startTermPointer, long endTermPointer, WarmUpByFieldBoundsStatus status, Term start, Term end,
      String name) throws IOException {
    byte[] buffer = new byte[4096];
    long position = startTermPointer;
    input.seek(position);
    long total = 0;
    long s = System.nanoTime();
    while (position < endTermPointer && !isClosed.get()) {
      int length = (int) Math.min(buffer.length, endTermPointer - position);
      input.readBytes(buffer, 0, length);
      position += length;
      total += length;
    }
    long e = System.nanoTime();
    status.complete(name, start, end, startTermPointer, endTermPointer, total, e - s, isClosed);
  }
}
