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
package org.apache.blur.manager.writer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Sorter;

public class TestingSeqSorting {

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Path p = new Path("hdfs://localhost:9000/testsort/data.seq");
    // URI uri = new File(".").getAbsoluteFile().toURI();
    // Path local = new Path(uri);

    // Path p = new Path(new Path(local, "tmp"), "data.seq");
    // {
    // Writer writer = SequenceFile.createWriter(conf,
    // Writer.valueClass(LongWritable.class),
    // Writer.keyClass(BytesWritable.class), Writer.file(p));
    //
    // LongWritable value = new LongWritable();
    // BytesWritable key = new BytesWritable();
    // Random random = new Random(1);
    // byte[] buf = new byte[50];
    // for (int i = 0; i < 10000000; i++) {
    // value.set(random.nextLong());
    // random.nextBytes(buf);
    // key.set(buf, 0, buf.length);
    // writer.append(key, value);
    // }
    // writer.close();
    // }

    // {
    // // io.seqfile.local.dir
    // URI uri = new File(".").getAbsoluteFile().toURI();
    // Path local = new Path(uri);
    //
    // conf.set("io.seqfile.local.dir", new Path(local, "tmp").toString());
    // Path tempDir = new Path(p.getParent(), "tmp");
    // Sorter sorter = new SequenceFile.Sorter(p.getFileSystem(conf),
    // BytesWritable.class, LongWritable.class, conf);
    // sorter.setMemory(10 * 1024 * 1024);
    // // Path tempDir = new Path(p.getParent(), "tmp");
    //
    // RawKeyValueIterator iterate = sorter.sortAndIterate(new Path[] { p },
    // tempDir, false);
    // Path sortedPath = new Path(p.getParent(), "sorted.seq");
    // Writer writer = SequenceFile.createWriter(conf,
    // Writer.valueClass(LongWritable.class),
    // Writer.keyClass(BytesWritable.class), Writer.file(sortedPath));
    // while (iterate.next()) {
    // DataOutputBuffer key = iterate.getKey();
    // byte[] keyData = key.getData();
    // int keyOffset = 0;
    // int keyLength = key.getLength();
    // ValueBytes val = iterate.getValue();
    // writer.appendRaw(keyData, keyOffset, keyLength, val);
    // }
    // writer.close();
    // }
    {
      conf.setInt("io.sort.factor", 1000);
      Sorter sorter = new SequenceFile.Sorter(p.getFileSystem(conf), BytesWritable.class, LongWritable.class, conf);
      sorter.setMemory(10 * 1024 * 1024);
      sorter.sort(new Path[] { p }, new Path(p.getParent(), "sorted.seq"), false);
    }

  }

}
