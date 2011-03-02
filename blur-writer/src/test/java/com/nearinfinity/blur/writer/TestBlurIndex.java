package com.nearinfinity.blur.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.replication.ReplicaHdfsDirectory;
import com.nearinfinity.blur.store.replication.ReplicationDaemon;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Row;

public class TestBlurIndex {

    public static void main(String[] args) throws InterruptedException, IOException {
        BlurAnalyzer analyzer = new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_30), "");
//        Directory dir = FSDirectory.open(new File("./index"));
        Directory dir = getDir2();
        
        final BlurIndex blurIndex = new BlurIndex();
        blurIndex.setAnalyzer(analyzer);
        blurIndex.setDirectory(dir);
        blurIndex.init();
        
        System.out.println("Opening reader.");
        
        IndexReader reader = blurIndex.getIndexReader();
        
        System.out.println("Starting writer.");
        
        final AtomicInteger count = new AtomicInteger();
        
        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000000; i++) {
                        try {
                            if (!blurIndex.replaceRow(genRows(count))) {
                                System.err.println("Did not index!!!");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        
        int startingDocCount = reader.numDocs();
        System.out.println("Started...");
        executorService.shutdown();
        int prevNumDocs = 0;
        long s = System.nanoTime();
        long s1 = s;
        while (!executorService.isTerminated()) {
            IndexReader newReader = blurIndex.getIndexReader();
            if (newReader != reader) {
                reader.close();
                reader = newReader;
            }
            int numDocs = reader.numDocs();
            long now = System.nanoTime();
            double avgSeconds = (now - s1) / 1000000000.0;
            double seconds = (now - s) / 1000000000.0;
            double avgRate = (numDocs - startingDocCount) / avgSeconds;
            double rate = (numDocs - prevNumDocs) / seconds;
            System.out.println("Row Count=" + count + ",Doc Count=" + numDocs + ",Doc Rate=" + rate + ",Doc Avg Rate=" + avgRate);
            Thread.sleep(3000);
            prevNumDocs = numDocs;
            s = now;
        }
        System.out.println("Finished in [" + (System.currentTimeMillis() - start) + "ms]");
    }
    
    private static Directory getDir2() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name","hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsDirPath = new Path("hdfs://localhost:9000/blur/tables/table/shard-00000");

        LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(new File("./tmp/cache1/"),new File("./tmp/cache2/"));
        localFileCache.init();
        
        Progressable progressable = new Progressable() {
            @Override
            public void progress() {
            }
        };
        
        ReplicationDaemon replicationDaemon = new ReplicationDaemon();
        replicationDaemon.setLocalFileCache(localFileCache);
        replicationDaemon.init();
        
        return new ReplicaHdfsDirectory("table", "shard-00000", hdfsDirPath, fileSystem, localFileCache, new NoLockFactory(), progressable, replicationDaemon);
    }
    
    private static Collection<Row> genRows(AtomicInteger count) {
        Collection<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < 10; i++) {
            rows.add(genRow());
            count.incrementAndGet();
        }
        return rows;
    }

    private static Row genRow() {
        Row row = new Row();
        row.id = UUID.randomUUID().toString();
        row.columnFamilies = getColumnFamilies();
        return row;
    }

    private static Set<ColumnFamily> getColumnFamilies() {
        Set<ColumnFamily> result = new HashSet<ColumnFamily>();
        for (int i = 0; i < 10; i++) {
            result.add(getColumnFamily("fam"+i));
        }
        return result;
    }

    private static ColumnFamily getColumnFamily(String name) {
        ColumnFamily family = new ColumnFamily();
        family.family = name;
        family.records = getRecords();
        return family;
    }

    private static Map<String, Set<Column>> getRecords() {
        Map<String, Set<Column>> records = new HashMap<String, Set<Column>>();
        for (int i = 0; i < 10; i++) {
            String redcordId = UUID.randomUUID().toString();
            records.put(redcordId, getColumns());
        }
        return records;
    }

    private static Set<Column> getColumns() {
        Set<Column> columns = new HashSet<Column>();
        for (int i = 0; i < 10; i++) {
            columns.add(getColumn("col" + i));
        }
        return columns;
    }

    private static Column getColumn(String name) {
        Column column = new Column();
        column.name = name;
        column.values = new ArrayList<String>();
        column.values.add("value");
        return column;
    }
    
}
