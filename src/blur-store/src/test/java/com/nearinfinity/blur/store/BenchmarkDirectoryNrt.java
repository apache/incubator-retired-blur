package com.nearinfinity.blur.store;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.blockcache.BlockCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;

public class BenchmarkDirectoryNrt {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException, InterruptedException {
    int numberOfBlocksPerBank = 8192;
    int blockSize = BlockDirectory.BLOCK_SIZE;
    int numberOfBanks = getNumberOfBanks(0.5f, numberOfBlocksPerBank, blockSize);
    BlockCache blockCache = new BlockCache(numberOfBanks, numberOfBlocksPerBank, blockSize, new BlurMetrics(new Configuration()),true);
    BlurMetrics metrics = new BlurMetrics(new Configuration());
    BlockDirectoryCache cache = new BlockDirectoryCache(blockCache, metrics);

    Path p = new Path("hdfs://localhost:9000/bench");
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    fs.delete(p, true);

    final HdfsDirectory dir = new HdfsDirectory(p);
    dir.setLockFactory(new NoLockFactory());

    BlockDirectory directory = new BlockDirectory("test", DirectIODirectory.wrap(dir), cache);

    while (true) {
      IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
      TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
      mergePolicy.setUseCompoundFile(false);
      int count = 0;
      int max = 100;
      long s = System.currentTimeMillis();
      IndexWriter writer = new IndexWriter(directory, conf);
      long as = System.currentTimeMillis();
      BlockingQueue<Collection<Document>> queue = new ArrayBlockingQueue<Collection<Document>>(1024);
      Indexer indexer = new Indexer(queue,writer);
      new Thread(indexer).start();
      for (int i = 0; i < 1000000; i++) {
        if (count >= max) {
          double aseconds = (System.currentTimeMillis()-as) / 1000.0;
          double arate = i / aseconds;
          double seconds = (System.currentTimeMillis()-s) / 1000.0;
          double rate = count / seconds;
          System.out.println("Total [" + i + "] Rate [" + rate + "] AvgRate [" + arate +
          		"] Doc count [" + indexer.getReader().numDocs() + "]");
          count = 0;
          s = System.currentTimeMillis();
        }
        queue.put(Arrays.asList(getDoc()));
        count++;
      }
      writer.close();
    }
  }
  
  private static class Indexer implements Runnable {
    
    private BlockingQueue<Collection<Document>> _queue;
    private AtomicBoolean _running = new AtomicBoolean(true);
    private IndexWriter _writer;
    private IndexReader _reader;

    public Indexer(BlockingQueue<Collection<Document>> queue, IndexWriter writer) throws CorruptIndexException, IOException {
      _queue = queue;
      _writer = writer;
      _reader = IndexReader.open(_writer, true);
    }
    
    public IndexReader getReader() {
      return _reader;
    }

    @Override
    public void run() {
      long cycleTime = 50000000;
      long start = System.nanoTime();
      while (_running.get()) {
        try {
          Collection<Document> docs = _queue.take();
          _writer.addDocuments(docs);
          if (start + cycleTime < System.nanoTime()) {
            IndexReader newReader = IndexReader.open(_writer, true);
            _reader.close();
            _reader = newReader;
            start = System.nanoTime();
          }
        } catch (InterruptedException e) {
          return;
        } catch (CorruptIndexException e) {
          e.printStackTrace();
          return;
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
      }
    }
  }

  private static Document getDoc() {
    Document document = new Document();
    document.add(new Field("name", UUID.randomUUID().toString(), Store.YES, Index.ANALYZED_NO_NORMS));
    return document;
  }

  public static int getNumberOfBanks(float heapPercentage, int numberOfBlocksPerBank, int blockSize) {
    long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    long targetBytes = (long) (max * heapPercentage);
    int slabSize = numberOfBlocksPerBank * blockSize;
    int slabs = (int) (targetBytes / slabSize);
    if (slabs == 0) {
      throw new RuntimeException("Minimum heap size is 512m!");
    }
    return slabs;
  }
}
