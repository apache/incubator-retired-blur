package org.apache.blur.thrift;

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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.BlurAnalyzer;
import org.apache.blur.manager.IndexManager;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlurShardServerTest {

  private BlurShardServer blurShardServer;

  @Before
  public void setUp() throws BlurException, IOException {
    IndexServer indexServer = getIndexServer();
    IndexManager indexManager = getIndexManager();
    indexManager.setIndexServer(indexServer);
    indexManager.setThreadCount(1);
    indexManager.init();
    blurShardServer = new BlurShardServer();
    blurShardServer.setIndexManager(indexManager);
    blurShardServer.setIndexServer(indexServer);
    blurShardServer.setConfiguration(new BlurConfiguration());
    blurShardServer.init();
  }

  @After
  public void tearDown() throws InterruptedException {
    blurShardServer.close();
  }

  @Test
  public void testNothingYet() {

  }

  private IndexServer getIndexServer() {
    return new IndexServer() {

      @Override
      public void close() {

      }

      @Override
      public BlurAnalyzer getAnalyzer(String table) {
        throw new RuntimeException("not impl");
      }

      @Override
      public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        throw new RuntimeException("not impl");
      }

      @Override
      public List<String> getShardList(String table) {
        throw new RuntimeException("not impl");
      }

      @Override
      public Similarity getSimilarity(String table) {
        throw new RuntimeException("not impl");
      }

      @Override
      public TABLE_STATUS getTableStatus(String table) {
        throw new RuntimeException("not impl");
      }

      @Override
      public String getNodeName() {
        throw new RuntimeException("no impl");
      }

      @Override
      public String getTableUri(String table) {
        throw new RuntimeException("no impl");
      }

      @Override
      public int getShardCount(String table) {
        throw new RuntimeException("no impl");
      }

      @Override
      public int getCompressionBlockSize(String table) {
        throw new RuntimeException("no impl");
      }

      @Override
      public CompressionCodec getCompressionCodec(String table) {
        throw new RuntimeException("no impl");
      }

      @Override
      public long getRecordCount(String table) throws IOException {
        throw new RuntimeException("no impl");
      }

      @Override
      public long getRowCount(String table) throws IOException {
        throw new RuntimeException("no impl");
      }

      @Override
      public long getTableSize(String table) throws IOException {
        throw new RuntimeException("no impl");
      }

      @Override
      public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
        throw new RuntimeException("no impl");
      }

      @Override
      public Map<String, ShardState> getShardState(String table) {
        throw new RuntimeException("not impl");
      }
    };
  }

  private IndexManager getIndexManager() {
    return new IndexManager() {

    };
  }

}
