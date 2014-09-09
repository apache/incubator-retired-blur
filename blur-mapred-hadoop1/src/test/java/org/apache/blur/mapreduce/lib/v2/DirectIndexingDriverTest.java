/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.mapreduce.lib.v2;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.junit.Test;

public class DirectIndexingDriverTest {

  @Test
  public void testIndexing() throws Exception {
    Configuration configuration = new Configuration();
    Path inputPath = new Path("./tmp/test_DirectIndexingDriverTest_input/");
    Path outputPath = new Path("./tmp/test_DirectIndexingDriverTest_output/");
    FileSystem fileSystem = inputPath.getFileSystem(configuration);
    createInputDocument(fileSystem, configuration, inputPath);
    DirectIndexingDriver directIndexingDriver = new DirectIndexingDriver();
    directIndexingDriver.setConf(configuration);
    assertEquals(0, directIndexingDriver.run(new String[] { inputPath.toString(), outputPath.toString() }));
  }

  public static void createInputDocument(FileSystem fileSystem, Configuration configuration, Path path)
      throws IOException {
    Writer writer = SequenceFile.createWriter(fileSystem, configuration, new Path(path, "data"), IntWritable.class,
        DocumentWritable.class);
    IntWritable docId = new IntWritable();
    DocumentWritable documentWritable = new DocumentWritable();
    int numberOfFields = 10;
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      docId.set(i);
      documentWritable.clear();
      populate(numberOfFields, random, documentWritable);
      writer.append(docId, documentWritable);
    }
    writer.close();
  }

  public static void populate(int numberOfFields, Random random, DocumentWritable documentWritable) {
    for (int i = 0; i < numberOfFields; i++) {
      long l = random.nextLong();
      documentWritable.add(new StringField("f" + i, Long.toString(l), Store.YES));
    }
  }
}
