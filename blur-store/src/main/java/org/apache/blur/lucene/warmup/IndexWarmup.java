package org.apache.blur.lucene.warmup;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.index.ExitableReader.ExitableFilterAtomicReader;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.warmup.IndexTracerResult.FILE_TYPE;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.OpenBitSet;

/**
 * IndexWarmup is used to pre-read portions of the index by field. Usage:<br/>
 * <br/>
 * int maxSampleSize = 1000; <br/>
 * Directory dir = FSDirectory.open(new File("/path/index"));<br/>
 * dir = new TraceableDirectory(dir);<br/>
 * DirectoryReader reader = DirectoryReader.open(dir);<br/>
 * <br/>
 * IndexWarmup indexWarmup = new IndexWarmup(new AtomicBoolean());<br/>
 * Map&lt;String, List&lt;IndexTracerResult&gt;&gt; sampleIndex =
 * indexWarmup.sampleIndex(reader, "");<br/>
 * indexWarmup.warm(reader, sampleIndex, "uuid", null);<br/>
 * indexWarmup.warm(reader, sampleIndex, "test", "test");<br/>
 * indexWarmup.warm(reader, sampleIndex, "nothing", null);<br/>
 * indexWarmup.warm(reader, sampleIndex, "id2", "tst");<br/>
 */
public class IndexWarmup {

  private static final Log LOG = LogFactory.getLog(IndexWarmup.class);
  private static final String SAMPLE_EXT = ".sample";
  private static final long _5_SECONDS = TimeUnit.SECONDS.toNanos(5);
  private static final int DEFAULT_THROTTLE = 2000000;

  private final AtomicBoolean _isClosed;
  private final int _maxSampleSize;
  private final long _maxBytesPerSec;
  private final AtomicBoolean _stop;

  public IndexWarmup(AtomicBoolean isClosed, AtomicBoolean stop, int maxSampleSize, long maxBytesPerSec) {
    _isClosed = isClosed;
    _stop = stop;
    _maxSampleSize = maxSampleSize;
    _maxBytesPerSec = maxBytesPerSec;
  }

  public IndexWarmup(AtomicBoolean isClosed, AtomicBoolean stop, int maxSampleSize) {
    this(isClosed, stop, maxSampleSize, DEFAULT_THROTTLE);
  }

  private static ThreadLocal<Boolean> runTrace = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  public static boolean isRunTrace() {
    return runTrace.get();
  }

  public static void enableRunTrace() {
    runTrace.set(Boolean.TRUE);
  }

  public static void disableRunTrace() {
    runTrace.set(Boolean.FALSE);
  }

  public void warmFile(IndexReader reader, Map<String, List<IndexTracerResult>> sampleIndex, String fieldName,
      String context) throws IOException {
    for (Entry<String, List<IndexTracerResult>> segment : sampleIndex.entrySet()) {
      warmFile(reader, segment.getValue(), fieldName, context);
    }
  }

  public void getFilePositionsToWarm(IndexReader reader, Map<String, List<IndexTracerResult>> sampleIndex,
      String fieldName, String context, Map<String, OpenBitSet> filePartsToWarm, int blockSize) throws IOException {
    for (Entry<String, List<IndexTracerResult>> segment : sampleIndex.entrySet()) {
      getFilePositionsToWarm(reader, segment.getValue(), fieldName, context, filePartsToWarm, blockSize);
    }
  }

  public void getFilePositionsToWarm(IndexReader reader, List<IndexTracerResult> traces, String fieldName,
      String context, Map<String, OpenBitSet> filePartsToWarm, int blockSize) throws IOException {
    int index = find(traces, fieldName);
    if (index < 0) {
      // not found
      return;
    }
    IndexTracerResult trace = traces.get(index);
    for (FILE_TYPE type : FILE_TYPE.values()) {
      if (trace.isFilePositionCaptured(type)) {
        long startingPosition = trace.getPosition(type);
        long endingPosition = Long.MAX_VALUE;
        int nextIndex = findNextSetTrace(traces, index + 1, type);
        if (nextIndex >= 0) {
          IndexTracerResult next = traces.get(nextIndex);
          endingPosition = next.getPosition(type);
        }
        String fileName = trace.getFileName(type);
        String segmentName = trace.getSegmentName();
        getFilePositionsToWarm(reader, segmentName, fileName, fieldName, startingPosition, endingPosition, context,
            filePartsToWarm, blockSize);
      }
    }
  }

  private void getFilePositionsToWarm(IndexReader reader, String segmentName, String fileName, String fieldName,
      long startingPosition, long endingPosition, String context, Map<String, OpenBitSet> filePartsToWarm, int blockSize)
      throws IOException {
    Directory dir = getDirectory(reader, segmentName, context);
    long fileLength = dir.fileLength(fileName);
    if (endingPosition == Long.MAX_VALUE) {
      endingPosition = fileLength - 1;
    }
    OpenBitSet openBitSet = filePartsToWarm.get(fileName);
    if (openBitSet == null) {
      openBitSet = new OpenBitSet((long) Math.ceil(fileLength / (double) blockSize));
      filePartsToWarm.put(fileName, openBitSet);
    }
    long startingBlock = startingPosition / blockSize;
    long endingBlock = endingPosition / blockSize;
    openBitSet.set(startingBlock, endingBlock);
  }

  void warmFile(IndexReader reader, List<IndexTracerResult> traces, String fieldName, String context)
      throws IOException {
    int index = find(traces, fieldName);
    if (index < 0) {
      // not found
      return;
    }
    IndexTracerResult trace = traces.get(index);
    for (FILE_TYPE type : FILE_TYPE.values()) {
      if (trace.isFilePositionCaptured(type)) {
        long startingPosition = trace.getPosition(type);
        long endingPosition = Long.MAX_VALUE;
        int nextIndex = findNextSetTrace(traces, index + 1, type);
        if (nextIndex >= 0) {
          IndexTracerResult next = traces.get(nextIndex);
          endingPosition = next.getPosition(type);
        }
        String fileName = trace.getFileName(type);
        String segmentName = trace.getSegmentName();
        warmFile(reader, segmentName, fileName, fieldName, startingPosition, endingPosition, context);
      }
    }
  }

  private void warmFile(IndexReader reader, String segmentName, String fileName, String fieldName,
      long startingPosition, long endingPosition, String context) throws IOException {
    Directory dir = getDirectory(reader, segmentName, context);
    if (dir == null) {
      LOG.info("Context [{0}] cannot find segment [{1}]", context, segmentName);
      return;
    }
    if (endingPosition == Long.MAX_VALUE) {
      endingPosition = dir.fileLength(fileName) - 1;
    }
    if (_isClosed.get() || _stop.get()) {
      LOG.info("Context [{0}] index closed", context);
      return;
    }
    long length = endingPosition - startingPosition;
    final long totalLength = length;
    IndexInput input = new ThrottledIndexInput(dir.openInput(fileName, IOContext.READ), _maxBytesPerSec);
    try {
      input.seek(startingPosition);
      byte[] buf = new byte[8192];
      long start = System.nanoTime();
      long bytesReadPerPass = 0;
      while (length > 0) {
        long now = System.nanoTime();
        if (start + _5_SECONDS < now) {
          double seconds = (now - start) / 1000000000.0;
          double rateMbPerSec = (bytesReadPerPass / seconds) / 1000 / 1000;
          double complete = (((double) totalLength - (double) length) / (double) totalLength) * 100.0;
          LOG.debug("Context [{3}] warming field [{0}] in file [{1}] is [{2}%] complete at rate of [{4} MB/s]",
              fieldName, fileName, complete, context, rateMbPerSec);
          start = System.nanoTime();
          bytesReadPerPass = 0;
          if (_isClosed.get()) {
            LOG.info("Context [{0}] index closed", context);
            return;
          }
        }
        int len = (int) Math.min(length, buf.length);
        input.readBytes(buf, 0, len, false);
        length -= len;
        bytesReadPerPass += len;
      }
      long now = System.nanoTime();
      double seconds = (now - start) / 1000000000.0;
      if (seconds < 1) {
        seconds = 1;
      }
      double rateMbPerSec = (bytesReadPerPass / seconds) / 1000 / 1000;
      LOG.info("Context [{3}] warming field [{0}] in file [{1}] is [{2}%] complete at rate of [{4} MB/s]", fieldName,
          fileName, 100, context, rateMbPerSec);
    } finally {
      input.close();
    }
  }

  private Directory getDirectory(IndexReader reader, String segmentName, String context) {
    if (reader instanceof AtomicReader) {
      return getDirectory((AtomicReader) reader, segmentName, context);
    }
    for (IndexReaderContext ctext : reader.getContext().leaves()) {
      if (_isClosed.get() || _stop.get()) {
        LOG.info("Context [{0}] index closed", context);
        return null;
      }
      AtomicReaderContext atomicReaderContext = (AtomicReaderContext) ctext;
      AtomicReader atomicReader = atomicReaderContext.reader();
      if (atomicReader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) atomicReader;
        if (segmentReader.getSegmentName().equals(segmentName)) {
          return segmentReader.directory();
        }
      }
    }
    return null;
  }

  private Directory getDirectory(IndexReader reader, String context) {
    if (reader instanceof AtomicReader) {
      return getDirectory((AtomicReader) reader, context);
    }
    for (IndexReaderContext ctext : reader.getContext().leaves()) {
      if (_isClosed.get() || _stop.get()) {
        LOG.info("Context [{0}] index closed", context);
        return null;
      }
      AtomicReaderContext atomicReaderContext = (AtomicReaderContext) ctext;
      AtomicReader atomicReader = atomicReaderContext.reader();
      if (atomicReader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) atomicReader;
        return segmentReader.directory();
      }
    }
    return null;
  }

  private Directory getDirectory(AtomicReader reader, String context) {
    if (reader instanceof ExitableFilterAtomicReader) {
      return getDirectory(((ExitableFilterAtomicReader) reader).getOriginalReader(), context);
    } else if (reader instanceof SegmentReader) {
      return ((SegmentReader) reader).directory();
    }
    return null;
  }

  private Directory getDirectory(AtomicReader atomicReader, String segmentName, String context) {
    if (atomicReader instanceof SegmentReader) {
      SegmentReader segmentReader = (SegmentReader) atomicReader;
      if (segmentReader.getSegmentName().equals(segmentName)) {
        return segmentReader.directory();
      }
    }
    return null;
  }

  private int findNextSetTrace(List<IndexTracerResult> traces, int startingIndex, FILE_TYPE type) {
    int size = traces.size();
    for (int i = startingIndex; i < size; i++) {
      IndexTracerResult trace = traces.get(i);
      if (trace.isFilePositionCaptured(type)) {
        return i;
      }
    }
    return -1;
  }

  private int find(List<IndexTracerResult> traces, String fieldName) {
    int index = 0;
    for (IndexTracerResult trace : traces) {
      if (trace.getField().equals(fieldName)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  public Map<String, List<IndexTracerResult>> sampleIndex(IndexReader reader, String context) throws IOException {
    Map<String, List<IndexTracerResult>> results = new HashMap<String, List<IndexTracerResult>>();
    for (IndexReaderContext ctext : reader.getContext().leaves()) {
      if (_isClosed.get() || _stop.get()) {
        LOG.info("Context [{0}] index closed", context);
        return null;
      }
      AtomicReaderContext atomicReaderContext = (AtomicReaderContext) ctext;
      AtomicReader atomicReader = atomicReaderContext.reader();
      results.putAll(sampleIndex(atomicReader, context));
    }
    return results;
  }

  public Map<String, List<IndexTracerResult>> sampleIndex(AtomicReader atomicReader, String context) throws IOException {
    Map<String, List<IndexTracerResult>> results = new HashMap<String, List<IndexTracerResult>>();
    if (atomicReader instanceof SegmentReader) {
      SegmentReader segmentReader = (SegmentReader) atomicReader;
      Directory directory = segmentReader.directory();
      if (!(directory instanceof TraceableDirectory)) {
        LOG.info("Context [{1}] cannot warmup directory [{0}] needs to be a TraceableDirectory.", directory, context);
        return results;
      }
      IndexTracer tracer = new IndexTracer((TraceableDirectory) directory, _maxSampleSize);
      String fileName = getSampleFileName(segmentReader.getSegmentName());
      List<IndexTracerResult> segmentTraces = new ArrayList<IndexTracerResult>();
      if (directory.fileExists(fileName)) {
        IndexInput input = directory.openInput(fileName, IOContext.READONCE);
        segmentTraces = read(input);
        input.close();
      } else {
        Fields fields = atomicReader.fields();
        for (String field : fields) {
          LOG.debug("Context [{1}] sampling field [{0}].", field, context);
          Terms terms = fields.terms(field);
          boolean hasOffsets = terms.hasOffsets();
          boolean hasPayloads = terms.hasPayloads();
          boolean hasPositions = terms.hasPositions();

          tracer.initTrace(segmentReader, field, hasPositions, hasPayloads, hasOffsets);
          IndexTracerResult result = tracer.runTrace(terms);
          segmentTraces.add(result);
        }
        if (_isClosed.get() || _stop.get()) {
          LOG.info("Context [{0}] index closed", context);
          return null;
        }
        IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT);
        write(segmentTraces, output);
        output.close();
      }
      results.put(segmentReader.getSegmentName(), segmentTraces);
    }
    return results;
  }

  public static String getSampleFileName(String segmentName) {
    return segmentName + SAMPLE_EXT;
  }

  private List<IndexTracerResult> read(IndexInput input) throws IOException {
    int count = input.readVInt();
    List<IndexTracerResult> results = new ArrayList<IndexTracerResult>(count);
    for (int i = 0; i < count; i++) {
      results.add(IndexTracerResult.read(input));
    }
    return results;
  }

  private void write(List<IndexTracerResult> segmentTraces, IndexOutput output) throws IOException {
    output.writeVInt(segmentTraces.size());
    for (IndexTracerResult r : segmentTraces) {
      r.write(output);
    }
  }

  public void warmFile(IndexReader indexReader, Map<String, OpenBitSet> filePartsToWarm, String context, int blockSize,
      int bufferSize) throws IOException {
    Directory directory = getDirectory(indexReader, context);
    ThrottledIndexInput input = null;
    long s = System.nanoTime();
    double _targetThrouhput = _maxBytesPerSec / 1000.0 / 1000.0;
    for (Entry<String, OpenBitSet> e : filePartsToWarm.entrySet()) {
      input = warmFile(directory, e.getKey(), e.getValue(), blockSize, context, bufferSize, input);
      if (input != null) {
        long end = System.nanoTime();
        double seconds = (end - s) / 1000000000.0;
        double rateMbPerSec = (input.getTotalBytesRead() / seconds) / 1000 / 1000;
        LOG.debug(
            "Context [{0}] warming file [{1}] is [{2}%] complete " + "at rate of [{3} MB/s] target was [{4} MB/s]",
            context, e.getKey(), 100, rateMbPerSec, _targetThrouhput);
      }
    }
    if (input != null) {
      long end = System.nanoTime();
      double seconds = (end - s) / 1000000000.0;
      double rateMbPerSec = (input.getTotalBytesRead() / seconds) / 1000 / 1000;
      LOG.info("Finished warming context [{2}] [{0} MB/s] target was [{1} MB/s]", rateMbPerSec, _targetThrouhput,
          context);
    }
  }

  private ThrottledIndexInput warmFile(Directory dir, String fileName, OpenBitSet bitSet, int blockSize,
      String context, int bufferSize, ThrottledIndexInput lastInput) throws IOException {
    if (bufferSize < blockSize) {
      throw new IOException("Buffer Size [" + bufferSize + "] cannot be less than Block Size [" + blockSize + "]");
    }
    if (_isClosed.get() || _stop.get()) {
      LOG.info("Context [{0}] index closed", context);
      return null;
    }
    ThrottledIndexInput input;
    if (lastInput == null) {
      input = new ThrottledIndexInput(dir.openInput(fileName, IOContext.READ), _maxBytesPerSec);
    } else {
      input = new ThrottledIndexInput(dir.openInput(fileName, IOContext.READ), _maxBytesPerSec,
          lastInput.getTotalBytesRead(), lastInput.getTotalSleepTime(), lastInput.getStartTime());
    }
    try {
      long length = input.length();
      final long totalAmountToBeRead = bitSet.capacity() * blockSize;
      byte[] buf = new byte[bufferSize];
      int maxNumberOfContiguousBlocks = bufferSize / blockSize;
      long start = System.nanoTime();
      final long originalStart = start;
      long bytesReadPerPass = 0;
      long totalBytesRead = 0;
      long blockPosition = 0;
      while ((blockPosition = bitSet.nextSetBit(blockPosition)) != -1) {
        long now = System.nanoTime();
        if (start + _5_SECONDS < now) {
          double seconds = (now - start) / 1000000000.0;
          double rateMbPerSec = (bytesReadPerPass / seconds) / 1000 / 1000;
          double complete = (((double) totalAmountToBeRead - (double) totalBytesRead) / (double) totalAmountToBeRead) * 100.0;
          LOG.debug("Context [{0}] warming file [{1}] is [{2}%] complete at rate of [{3} MB/s]", context, fileName,
              complete, rateMbPerSec);
          start = System.nanoTime();
          bytesReadPerPass = 0;
          if (_isClosed.get()) {
            LOG.info("Context [{0}] index closed", context);
            return null;
          }
        }
        long currentPosition = input.getFilePointer();
        long newPosition = blockPosition * blockSize;
        if (newPosition != currentPosition) {
          input.seek(newPosition);
        }
        int numberOfContiguousBlocks = getNumberOfContiguousBlock(maxNumberOfContiguousBlocks, bitSet, blockPosition);
        int len = (int) Math.min(blockSize * numberOfContiguousBlocks, length - newPosition);
        input.readBytes(buf, 0, len);
        bytesReadPerPass += len;
        totalBytesRead += len;
        blockPosition += numberOfContiguousBlocks;
        if (_isClosed.get()) {
          LOG.info("Context [{0}] index closed", context);
          return null;
        }
      }
      long now = System.nanoTime();
      double seconds = (now - originalStart) / 1000000000.0;
      double rateMbPerSec = (totalBytesRead / seconds) / 1000 / 1000;
      LOG.debug("Context [{0}] warming file [{1}] is [{2}%] complete at rate of [{3} MB/s]", context, fileName, 100,
          rateMbPerSec);
      return input;
    } finally {
      input.close();
    }
  }

  private int getNumberOfContiguousBlock(int maxNumberOfContiguousBlocks, OpenBitSet bitSet, long blockPosition) {
    for (int i = 0; i < maxNumberOfContiguousBlocks; i++) {
      if (!bitSet.get(i + blockPosition)) {
        return i;
      }
    }
    return maxNumberOfContiguousBlocks;
  }
}
