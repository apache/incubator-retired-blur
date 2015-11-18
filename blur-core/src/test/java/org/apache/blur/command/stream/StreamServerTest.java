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
package org.apache.blur.command.stream;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.command.IndexContext;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.IndexAction;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

@SuppressWarnings("serial")
public class StreamServerTest implements Serializable {

  @Test
  public void testServer() throws StreamException, IOException {
    Closer closer = Closer.create();
    try {
      File tmpFile = new File("./target/tmp/StreamServerTest");
      tmpFile.mkdirs();
      IndexServer indexServer = new TestIndexServer();
      StreamProcessor streamProcessor = new StreamProcessor(indexServer, tmpFile);
      int timeout = 3000000;
      String classLoaderId = UUID.randomUUID().toString();

      StreamServer server = closer.register(new StreamServer(0, 100, streamProcessor));
      server.start();
      int port = server.getPort();
      StreamClient client = closer.register(new StreamClient("localhost", port, timeout));
      assertFalse(client.isClassLoaderAvailable(classLoaderId));
      client.loadJars(classLoaderId, getTestJar());

      String table = "test";
      String shard = "shard";
      String user = "test";
      Map<String, String> userAttributes = new HashMap<String, String>();
      StreamSplit split = new StreamSplit(table, shard, classLoaderId, user, userAttributes);
      Iterable<String> it = client.executeStream(split, new StreamFunction<String>() {
        @Override
        public void call(IndexContext indexContext, StreamWriter<String> writer) throws Exception {
          writer.write("test");
        }
      });
      Iterator<String> iterator = it.iterator();
      assertTrue(iterator.hasNext());
      assertEquals("test", iterator.next());
      assertFalse(iterator.hasNext());

    } finally {
      closer.close();
    }
  }

  @Test
  public void testServerInjectError() throws IOException {
    Closer closer = Closer.create();
    try {
      File tmpFile = new File("./target/tmp/StreamServerTest");
      tmpFile.mkdirs();
      IndexServer indexServer = new TestIndexServer();
      StreamProcessor streamProcessor = new StreamProcessor(indexServer, tmpFile);
      int timeout = 3000000;
      String classLoaderId = UUID.randomUUID().toString();

      StreamServer server = closer.register(new StreamServer(0, 100, streamProcessor));
      server.start();
      int port = server.getPort();
      StreamClient client = closer.register(new StreamClient("localhost", port, timeout));
      assertFalse(client.isClassLoaderAvailable(classLoaderId));
      client.loadJars(classLoaderId, getTestJar());

      String table = "test";
      String shard = "shard";
      String user = "test";
      Map<String, String> userAttributes = new HashMap<String, String>();
      StreamSplit split = new StreamSplit(table, shard, classLoaderId, user, userAttributes);
      try {
        Iterable<String> it = client.executeStream(split, new StreamFunction<String>() {
          @Override
          public void call(IndexContext indexContext, StreamWriter<String> writer) throws Exception {
            Class.forName("errorclass");
          }
        });
        Iterator<String> iterator = it.iterator();
        if (iterator.hasNext()) {
          iterator.next();
        }
        fail();
      } catch (StreamException e) {
        Throwable cause = e.getCause();
        cause.printStackTrace();
      }

    } finally {
      closer.close();
    }
  }

  private String getTestJar() {
    String property = System.getProperty("java.class.path");
    Splitter splitter = Splitter.on(':');
    for (String s : splitter.split(property)) {
      if (s.endsWith(".jar")) {
        return s;
      }
    }
    throw new RuntimeException("No jars found?");
  }

  public static class TestIndexServer implements IndexServer {

    @Override
    public SortedSet<String> getShardListCurrentServerOnly(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public Map<String, BlurIndex> getIndexes(String table) throws IOException {
      Map<String, BlurIndex> map = new HashMap<String, BlurIndex>();
      BlurIndex value = getBlurIndex();
      map.put("shard", value);
      return map;
    }

    @Override
    public String getNodeName() {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public long getRecordCount(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public long getRowCount(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public long getTableSize(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public void close() throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public Map<String, ShardState> getShardState(String table) {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public long getSegmentImportInProgressCount(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public long getSegmentImportPendingCount(String table) throws IOException {
      throw new RuntimeException("Not implemented.");
    }

  }

  public static BlurIndex getBlurIndex() throws IOException {
    String shard = "shard";
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName("test");
    tableDescriptor.setTableUri("file:///tmp");
    TableContext tableContext = TableContext.create(tableDescriptor);
    ShardContext shardContext = ShardContext.create(tableContext, shard);
    return new BlurIndex(shardContext, null, null, null, null, null, null, null, null, 0) {

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
      public long getSegmentImportPendingCount() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportInProgressCount() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getOnDiskSize() throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public IndexSearcherCloseable getIndexSearcher() throws IOException {
        return getIndexSearcherCloseable();
      }

      @Override
      public void finishBulkMutate(String bulkId, boolean apply, boolean blockUntilComplete) throws IOException {
        throw new RuntimeException("Not implemented.");
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

      @Override
      public void addBulkMutate(String bulkId, RowMutation mutation) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
  }

  protected static IndexSearcherCloseable getIndexSearcherCloseable() {
    return new IndexSearcherCloseable() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void setSimilarity(Similarity similarity) {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void search(Query query, Collector collector) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public TopDocs search(Query query, int i) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public Query rewrite(Query query) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public IndexReader getIndexReader() {
        return null;
      }
    };
  }

}
