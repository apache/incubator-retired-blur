/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.analysis;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Field.Store;
import org.junit.Test;

public class BlurAnalyzerTest {
    
    private String s = "{" +
    "\"aliases\":[{\"standard\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}]," +
    "\"fulltext\":{\"a\":[\"b\"],\"b\":[\"c\"]}," +
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
    
    @Test
    public void testGetSubFields() throws IOException {
        BlurAnalyzer analyzer = BlurAnalyzer.create(s);
        assertNull(analyzer.getSubIndexNames("b.d"));
        Set<String> subIndexNames = analyzer.getSubIndexNames("b.c");
        TreeSet<String> set = new TreeSet<String>();
        set.add("b.c.sub1");
        set.add("b.c.sub2");
        assertEquals(set, subIndexNames);
    }
    
    @Test
    public void testFullTextFields() throws IOException {
        BlurAnalyzer analyzer = BlurAnalyzer.create(s);
        assertTrue(analyzer.isFullTextField("a.b"));
        assertFalse(analyzer.isFullTextField("a.d"));
    }

    private File newFile(String s) throws IOException {
        File file = File.createTempFile("test", ".js");
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(s.getBytes());
        outputStream.close();
        return file;
    }

}
