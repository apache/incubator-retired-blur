package org.apache.blur.mapreduce.lib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MiniHadoopClusterManager;

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

public class Test {

  public static void main(String[] args) throws FileNotFoundException, IOException, URISyntaxException {
    MiniHadoopClusterManager manager = new MiniHadoopClusterManager();
    String[] sargs = new String[]{"-D" + MiniDFSCluster.HDFS_MINIDFS_BASEDIR + "=./dfs-mini-tmp"};
    manager.run(sargs);
    manager.start();
  

  }

}
