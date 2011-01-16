package com.nearinfinity.blur.analysis;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Field.Store;
import org.junit.Test;

public class BlurAnalyzerTest {
    
    private String s = "{" +
    "\"aliases\":[{\"standard\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}]," +
    "\"default\":\"standard\"," +
    "\"fields\":{" +
        "\"a\":{" +
            "\"b\":\"standard\"" +
        "}," +
        "\"b\":{" +
            "\"c\":[" +
                "\"standard\"," +
                "{\"sub1\":\"standard\"}," +
                "{\"sub2\":\"standard\"}" +
            "]" +
        "}" +
    "}" +
    "}";
    
    @Test
    public void testString() throws IOException {
        BlurAnalyzer.create(s);
    }
    
    @Test
    public void testInputStream() throws IOException {
        BlurAnalyzer.create(new ByteArrayInputStream(s.getBytes()));
    }
    
    @Test
    public void testFile() throws IOException {
        File file = newFile(s);
        BlurAnalyzer.create(file);
        file.delete();
    }
    
    @Test
    public void testPath() throws IOException {
        File file = newFile(s);
        BlurAnalyzer.create(new Path(file.getAbsolutePath()));
        file.delete();
    }
    
    @Test
    public void testStoringOfField() throws IOException {
        BlurAnalyzer analyzer = BlurAnalyzer.create(s);
        assertEquals(Store.NO,analyzer.getStore("b.c.sub1"));
        assertEquals(Store.YES,analyzer.getStore("b.c"));
    }

    private File newFile(String s) throws IOException {
        File file = File.createTempFile("test", ".js");
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(s.getBytes());
        outputStream.close();
        return file;
    }

}
