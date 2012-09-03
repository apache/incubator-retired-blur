package com.nearinfinity.blur.store.hdfs;

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
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;

public class CopyFromHdfsLocal {

  public static void main(String[] args) throws IOException {
    Path path = new Path(args[0]);
    HdfsDirectory src = new HdfsDirectory(path);

    for (String name : src.listAll()) {
      System.out.println(name);
    }

    CompressedFieldDataDirectory compressedDirectory = new CompressedFieldDataDirectory(src, new DefaultCodec(), 32768);
    Directory dest = FSDirectory.open(new File(args[1]));

    for (String name : compressedDirectory.listAll()) {
      compressedDirectory.copy(dest, name, name);
    }

  }

}
