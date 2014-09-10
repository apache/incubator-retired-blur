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
package org.apache.blur.command;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.IndexAction;
import org.apache.blur.server.IndexSearcherClosable;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurUtil;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ShardCommandManagerTest {

  private static final Directory EMPTY_INDEX;

  static {
    EMPTY_INDEX = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_43, new KeywordAnalyzer());
    try {
      new IndexWriter(EMPTY_INDEX, conf).close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ShardCommandManager _manager;

  @Before
  public void setup() throws IOException {
    _manager = new ShardCommandManager(getIndexServer(), 10, 1000);
  }

  @After
  public void teardown() throws IOException {
    _manager.close();
  }

  @Test
  public void testShardCommandManagerNormalWait() throws IOException, TimeoutException {
    TableContext tableContext = getTableContext();
    Response response;
    ExecutionId executionId = null;

    Args args = new Args();
    args.set("seconds", 5);

    while (true) {
      try {
        if (executionId == null) {
          response = _manager.execute(tableContext, "wait", args);
        } else {
          response = _manager.reconnect(executionId);
        }
        break;
      } catch (TimeoutException te) {
        executionId = te.getExecutionId();
      }
    }
    System.out.println(response);
  }

  @Test
  public void testShardCommandManagerNormalWithCancel() throws IOException, TimeoutException {
    TableContext tableContext = getTableContext();
    Response response;
    ExecutionId executionId = null;

    Args args = new Args();
    args.set("seconds", 5);

    try {
      response = _manager.execute(tableContext, "wait", args);
    } catch (TimeoutException te) {
      _manager.cancel(te.getExecutionId());
      // some how validate the threads have cancelled.
    }

  }

  private TableContext getTableContext() {
    return TableContext.create(getTableDescriptor());
  }

  private TableDescriptor getTableDescriptor() {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setShardCount(3);
    tableDescriptor.setTableUri("file:///tmp/");
    return tableDescriptor;
  }

  private IndexServer getIndexServer() {
    return new IndexServer() {
      @Override
      public Map<String, BlurIndex> getIndexes(String table) throws IOException {
        Map<String, BlurIndex> indexes = new HashMap<String, BlurIndex>();
        for (int i = 0; i < 3; i++) {
          String shardName = BlurUtil.getShardName(i);
          indexes.put(shardName, getNullBlurIndex(shardName));
        }
        return indexes;
      }

      @Override
      public long getTableSize(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Map<String, ShardState> getShardState(String table) {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> getShardList(String table) {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getRowCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getRecordCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String getNodeName() {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void close() throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
  }

  protected BlurIndex getNullBlurIndex(String shard) throws IOException {
    ShardContext shardContext = ShardContext.create(getTableContext(), shard);
    return new BlurIndex(shardContext, null, null, null, null, null) {

      @Override
      public void removeSnapshot(String name) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void refresh() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void process(IndexAction indexAction) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void optimize(int numberOfSegmentsPerShard) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public AtomicBoolean isClosed() {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public List<String> getSnapshots() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public IndexSearcherClosable getIndexSearcher() throws IOException {
        IndexReader reader = getEmtpyReader();
        return new IndexSearcherClosable(reader, null) {

          @Override
          public Directory getDirectory() {
            return getEmtpyDirectory();
          }

          @Override
          public void close() throws IOException {

          }
        };
      }

      @Override
      public void enqueue(List<RowMutation> mutations) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void createSnapshot(String name) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void close() throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
  }

  protected IndexReader getEmtpyReader() throws IOException {
    return DirectoryReader.open(getEmtpyDirectory());
  }

  protected Directory getEmtpyDirectory() {
    return EMPTY_INDEX;
  }

}
