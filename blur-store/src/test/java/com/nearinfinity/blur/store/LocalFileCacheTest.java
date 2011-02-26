package com.nearinfinity.blur.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.blur.store.cache.LocalFileCacheCheck;
import com.nearinfinity.blur.store.cache.LocalFileCache;

public class LocalFileCacheTest {
    
    private static final File CACHE_BASE = new File("./tmp-test");
    private static final File CACHE1_FILE = new File(CACHE_BASE,"cache1");
    private static final File CACHE2_FILE = new File(CACHE_BASE,"cache2");
    private static final File CACHE3_FILE = new File(CACHE_BASE,"cache3");
    private static final File CACHE4_FILE = new File(CACHE_BASE,"cache4");
    private LocalFileCache localFileCache;
    
    @BeforeClass
    public static void clear() {
        LocalFileCache.rm(CACHE_BASE);
    }

    @Before
    public void setup() {
        localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(CACHE1_FILE,CACHE2_FILE);
        localFileCache.open();
    }
    
    @After
    public void teardown() {
        localFileCache.close();
    }
    
    @Test
    public void testGetLocalFile() throws IOException {
        File file = localFileCache.getLocalFile("test", "f1");
        writeBytes(file,1);
        File file2 = localFileCache.getLocalFile("test", "f1");
        assertEquals(1,file2.length());
    }
    
    @Test
    public void testDelete() throws IOException {
        assertEquals(1,count(CACHE1_FILE.listFiles()) + count(CACHE2_FILE.listFiles()));
        localFileCache.delete("test");
        assertEquals(0,count(CACHE1_FILE.listFiles()) + count(CACHE2_FILE.listFiles()));
    }
    
    @Test
    public void testGc() throws IOException, InterruptedException {
        LocalFileCache test = new LocalFileCache();
        test.setPotentialFiles(CACHE3_FILE,CACHE4_FILE);
        test.setGcWaitPeriod(TimeUnit.SECONDS.toMillis(5));
        test.setLocalFileCacheCheck(new LocalFileCacheCheck() {
            @Override
            public boolean isBeingServed(String dirName, String name) throws IOException {
                if (name.startsWith("keep")) {
                    return true;
                }
                return false;
            }
        });
        test.open();
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        
        writeBytes(test.getLocalFile("gctest", "keeper"),1);
        writeBytes(test.getLocalFile("gctest", "goner"),1);
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));
        
        assertTrue(test.getLocalFile("gctest", "keeper").exists());
        assertFalse(test.getLocalFile("gctest", "goner").exists());
        
        test.close();
    }

    private int count(File[] listFiles) {
        if (listFiles == null) {
            return 0;
        }
        return listFiles.length;
    }

    private void writeBytes(File file, long length) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        for (long l = 0; l < length; l++) {
            outputStream.write(1);
        }
        outputStream.close();
        
    }

}
