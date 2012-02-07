package com.nearinfinity.blur.store;

import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.metrics.BlurMetrics;
import com.nearinfinity.blur.store.blockcache.BlockCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;
import com.nearinfinity.blur.store.hdfs.HdfsDirectory;
import com.nearinfinity.blur.store.lock.ZookeeperLockFactory;
public class BenchmarkDirectory {

  public static void main(String[] args) throws IOException {
    int blockSize = BlockDirectory.BLOCK_SIZE;
    long totalMemory = BlockCache._128M * 2;
    int slabSize = (int) (totalMemory / 2);
    
    BlockCache blockCache = new BlockCache(new BlurMetrics(new Configuration()),true,totalMemory,slabSize,blockSize);
    BlurMetrics metrics = new BlurMetrics(new Configuration());
    BlockDirectoryCache cache = new BlockDirectoryCache(blockCache, metrics);
    
    ZooKeeper zooKeeper = new ZooKeeper("localhost", 30000, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        
      }
    });
    
    ZookeeperLockFactory factory = new ZookeeperLockFactory(zooKeeper, "/test-zk-lock", "shard-0", "localhost:40020");

    Path p = new Path("hdfs://localhost:9000/bench");
    FileSystem fs = FileSystem.get(p.toUri(), new Configuration());
    fs.delete(p, true);

    final HdfsDirectory dir = new HdfsDirectory(p);
    dir.setLockFactory(factory);
    
    BlockDirectory directory = new BlockDirectory("test", DirectIODirectory.wrap(dir), cache);

    while (true) {
      long s, e;

      s = System.currentTimeMillis();
      IndexWriterConfig conf = new IndexWriterConfig(LUCENE_VERSION, new StandardAnalyzer(LUCENE_VERSION));
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
      System.out.println("Docs " + reader.numDocs());
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
