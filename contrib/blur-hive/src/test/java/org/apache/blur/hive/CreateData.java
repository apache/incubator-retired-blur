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
package org.apache.blur.hive;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateData {

  private static final String SEP = new String(new char[] { 1 });

  public static void main(String[] args) throws IOException {
    Path path = new Path("hdfs://localhost:9000/user/hive/warehouse/test.db/input_data/data");
    Configuration configuration = new Configuration();
    FileSystem fileSystem = path.getFileSystem(configuration);
    FSDataOutputStream outputStream = fileSystem.create(path);
    PrintWriter print = new PrintWriter(outputStream);
    int rows = 100000;
    for (int i = 0; i < rows; i++) {
      String s = Integer.toString(i);
      print.print(s);
      print.print(SEP);
      print.print(s + "-" + System.currentTimeMillis());
      for (int c = 0; c < 10; c++) {
        print.print(SEP);
        print.print(s);
      }
      print.println();
    }
    print.close();
  }

}
