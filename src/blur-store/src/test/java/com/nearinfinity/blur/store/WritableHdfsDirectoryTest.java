package com.nearinfinity.blur.store;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
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

import com.nearinfinity.blur.store.cache.LocalFileCache;

public class WritableHdfsDirectoryTest {
    
    private LocalFileCache localFileCache;
    private WritableHdfsDirectory directory;

    @Before
    public void setup() throws IOException {
        File tmp = new File("./tmp");
        File file = new File("./tmp/base");
        File cache = new File("./tmp/cache");
        URI uri = file.toURI();
        HdfsDirectoryTest.rm(tmp);
        
        Path hdfsDirPath = new Path(uri.toString());
        localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(cache);
        localFileCache.init();
        LockFactory lockFactory = new NoLockFactory();
        directory = new WritableHdfsDirectory("test", hdfsDirPath, localFileCache, lockFactory);
    }
    
    @After
    public void teardown() throws IOException {
        directory.close();
        localFileCache.close();
    }
    
    @Test
    public void testWritableHdfsDir() throws IOException {
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
    public void testWritableHdfsDirWithIndex() throws IOException {
        IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
        writer.setUseCompoundFile(false);
        writer.addDocument(getDoc());
        writer.close();
        
        IndexReader reader = IndexReader.open(directory);
        assertEquals(1,reader.numDocs());
        reader.close();
    }

    private Document getDoc() {
        Document document = new Document();
        document.add(new Field("name","value",Store.YES,Index.ANALYZED_NO_NORMS));
        return document;
    }

}
