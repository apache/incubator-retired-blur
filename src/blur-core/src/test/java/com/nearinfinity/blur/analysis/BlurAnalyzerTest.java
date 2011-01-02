package com.nearinfinity.blur.analysis;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class BlurAnalyzerTest {
    private String s = "{" +
    "\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
    "\"fields\":{" +
        "\"a\":{" +
            "\"b\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"" +
        "}," +
        "\"b\":{" +
            "\"c\":[" +
                "\"org.apache.lucene.analysis.standard.StandardAnalyzer\"," +
                "{\"sub1\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}," +
                "{\"sub2\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}" +
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

    private File newFile(String s) throws IOException {
        return File.createTempFile("test", ".js");
    }

}
