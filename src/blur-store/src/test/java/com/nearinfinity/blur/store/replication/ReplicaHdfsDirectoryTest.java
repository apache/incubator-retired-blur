package com.nearinfinity.blur.store.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.store.HdfsDirectoryTest;
import com.nearinfinity.blur.store.cache.HdfsUtil;
import com.nearinfinity.blur.store.cache.LocalFileCache;

public class ReplicaHdfsDirectoryTest {

    private LocalFileCache localFileCache;
    private ReplicaHdfsDirectory directory;
    private String table = "table";
    private String shard = "shard";
    private File cache;
    private ReplicationDaemon replicationDaemon;

    @Before
    public void setup() throws IOException {
        File tmp = new File("./tmp");
        File file = new File("./tmp/base");
        cache = new File("./tmp/cache");
        URI uri = file.toURI();
        HdfsDirectoryTest.rm(tmp);
        
        Path hdfsDirPath = new Path(uri.toString());
        localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(cache);
        localFileCache.init();
        
        LockFactory lockFactory = new NoLockFactory();
        replicationDaemon = new ReplicationDaemon();
        replicationDaemon.setLocalFileCache(localFileCache);
        replicationDaemon.init();
        ReplicationStrategy strategy = new ReplicationStrategy() {
            @Override
            public boolean replicateLocally(String table, String name) {
                if (name.endsWith(".fdt") || name.endsWith(".fdz")) {
                    return false;
                }
                return true;
            }
        };
        
        Progressable progress = new Progressable() {
            @Override
            public void progress() {
                
            }
        };
        directory = new ReplicaHdfsDirectory(table,shard,hdfsDirPath,localFileCache,lockFactory,progress,replicationDaemon,strategy);
    }
    
    @After
    public void teardown() throws IOException {
        directory.close();
        localFileCache.close();
        replicationDaemon.close();
    }
    
    @Test
    public void testReplicaHdfsDir() throws IOException {
        String name = "testfile.test";
        
        IndexOutput output = directory.createOutput(name);
        output.writeLong(123421);
        output.close();
        
        IndexInput input1 = directory.openInput(name);
        assertEquals(123421,input1.readLong());
        input1.close();
        
        directory.sync(name);
        
        IndexInput input2 = directory.openInput(name);
        assertEquals(123421,input2.readLong());
        input2.close();
        
        directory.close();
        localFileCache.close();
    }
    
    @Test
    public void testReplicaHdfsDirWithIndex() throws IOException, InterruptedException {
        IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
        writer.setUseCompoundFile(false);
        writer.addDocument(getDoc());
        writer.close();
        
        String dirName = HdfsUtil.getDirName(table, shard);
        localFileCache.delete(dirName);
        
        assertFalse(new File(cache,dirName).exists());
        
        IndexReader reader = IndexReader.open(directory);
        assertEquals(1,reader.numDocs());
        reader.close();
        
        Thread.sleep(1000);
        
        writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
        writer.setUseCompoundFile(false);
        writer.addDocument(getDoc());
        writer.close();
        
        Thread.sleep(1000);
        //validate the files are local now
        
        for (File f : new File(cache,dirName).listFiles()) {
            System.out.println(f.getName());
        }
    }

    private Document getDoc() {
        Document document = new Document();
        document.add(new Field("name","value",Store.YES,Index.ANALYZED_NO_NORMS));
        return document;
    }
    
}
