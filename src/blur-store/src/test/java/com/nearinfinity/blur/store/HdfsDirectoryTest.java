package com.nearinfinity.blur.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class HdfsDirectoryTest {
    
    @Test
    public void testWritingAndReadingAFile() throws IOException {
        File file = new File("./tmp");
        rm(file);
        URI uri = file.toURI();
        Path hdfsDirPath = new Path(uri.toString());
        HdfsDirectory directory = new HdfsDirectory(hdfsDirPath);
        IndexOutput output = directory.createOutput("testing.test");
        output.writeInt(12345);
        output.flush();
        output.close();
        
        IndexInput input = directory.openInput("testing.test");
        assertEquals(12345,input.readInt());
        input.close();
        
        String[] listAll = directory.listAll();
        assertEquals(1, listAll.length);
        assertEquals("testing.test", listAll[0]);
        
        assertEquals(4, directory.fileLength("testing.test"));
        
        directory.rename("testing.test", "testing.test.new");
        
        IndexInput input1 = directory.openInput("testing.test.new");
        
        IndexInput input2 = (IndexInput) input1.clone();
        assertEquals(12345,input2.readInt());
        input2.close();
        
        assertEquals(12345,input1.readInt());
        input1.close();
        
        
        
        assertFalse(directory.fileExists("testing.test"));
        assertTrue(directory.fileExists("testing.test.new"));
        directory.deleteFile("testing.test.new");
        assertFalse(directory.fileExists("testing.test.new"));
    }

    public static void rm(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rm(f);
            }
        }
        file.delete();
    }

}
