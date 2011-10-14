package com.nearinfinity.blur.store;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.blockcache.BlockCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;
import com.nearinfinity.blur.store.blockcache.Cache;

public class BenchmarkDirectory {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    int numberOfBlocksPerBank = 8192;
    int blockSize = BlockDirectory.BLOCK_SIZE;
    int numberOfBanks = getNumberOfBanks(0.5f, numberOfBlocksPerBank, blockSize);
    BlockCache blockCache = new BlockCache(numberOfBanks, numberOfBlocksPerBank, blockSize);
    BlurMetrics metrics = new BlurMetrics(new Configuration());
    BlockDirectoryCache cache = new BlockDirectoryCache(blockCache, metrics);

    Path p = new Path("hdfs://localhost:9000/bench");
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    fs.delete(p, true);

    final HdfsDirectory dir = new HdfsDirectory(p);
    dir.setLockFactory(new NoLockFactory());

    final Map<String, byte[]> map = Collections.synchronizedMap(new LRUMap(8192));

//    Cache cache = new Cache() {
//
//      @Override
//      public void update(String name, long blockId, byte[] buffer) {
//        map.put(name + blockId, copy(buffer));
//      }
//      
//      private byte[] copy(byte[] buffer) {
//        byte[] b = new byte[buffer.length];
//        System.arraycopy(buffer, 0, b, 0, buffer.length);
//        return b;
//      }
//
//      @Override
//      public boolean fetch(String name, long blockId, int blockOffset, byte[] b, int off, int lengthToReadInBlock) {
//        byte[] data = map.get(name + blockId);
//        if (data == null) {
//          return false;
//        }
//        System.arraycopy(data, blockOffset, b, off, lengthToReadInBlock);
//        return true;
//      }
//
//      @Override
//      public void delete(String name) {
//        
//      }
//    };
    
    BlockDirectory directory = new BlockDirectory("test", DirectIODirectory.wrap(dir), cache);

    while (true) {
      long s, e;

      s = System.currentTimeMillis();
      IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
      TieredMergePolicy mergePolicy = (TieredMergePolicy) conf.getMergePolicy();
      mergePolicy.setUseCompoundFile(false);
      IndexWriter writer = new IndexWriter(directory, conf);
      for (int i = 0; i < 1000000; i++) {
        writer.addDocument(getDoc());
      }
      writer.close();
      e = System.currentTimeMillis();
      System.out.println("Indexing " + (e - s));

      IndexReader reader = IndexReader.open(directory);
      System.out.println("Dpcs " + reader.numDocs());
      TermEnum terms = reader.terms();
      List<Term> sample = new ArrayList<Term>();
      int limit = 1000;
      Random random = new Random();
      SAMPLE:
      while (terms.next()) {
        if (sample.size() < limit) {
          if (random.nextInt() % 7 == 0) {
            sample.add(terms.term());
          }
        } else {
          break SAMPLE;
        }
      }
      terms.close();

      System.out.println("Sampling complete [" + sample.size() + "]");
      IndexSearcher searcher = new IndexSearcher(reader);
      long total = 0;
      long time = 0;
      int search = 10;
      for (int i = 0; i < search; i++) {
        s = System.currentTimeMillis();
        TopDocs topDocs = searcher.search(new TermQuery(sample.get(random.nextInt(sample.size()))), 10);
        total += topDocs.totalHits;
        e = System.currentTimeMillis();
        time += (e - s);
      }
      System.out.println("Searching " + time + " " + (time / (double) search));
      for (int i = 0; i < 10; i++) {
        s = System.currentTimeMillis();
        TopDocs topDocs = searcher.search(new WildcardQuery(new Term("name", "fff*0*")), 10);
        e = System.currentTimeMillis();
        System.out.println(topDocs.totalHits + " " + (e - s));
      }
      reader.close();
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
