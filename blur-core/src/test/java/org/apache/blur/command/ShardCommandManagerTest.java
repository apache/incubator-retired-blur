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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.lucene.search.IndexSearcherCloseableBase;
import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.IndexAction;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.server.TableContextFactory;
import org.apache.blur.thrift.generated.Arguments;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.ShardState;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.ShardUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
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
  private File _tmpPath = new File("./target/tmp/ShardCommandManagerTest/tmp");
  private String _commandPath = "./target/tmp/ShardCommandManagerTest/command";
  private ShardCommandManager _manager;
  private Configuration _config;

  @Before
  public void setup() throws IOException {
    _config = new Configuration();
    _manager = new ShardCommandManager(getIndexServer(), null, null, 10, 10, 1000, _config);
  }

  @After
  public void teardown() throws IOException {
    _manager.close();
  }

  @Test
  public void testGetCommands() {
    Map<String, BigInteger> commands = _manager.getCommands();
    assertEquals(4, commands.size());
    assertTrue(commands.containsKey("wait"));
    assertTrue(commands.containsKey("error"));
    assertTrue(commands.containsKey("RunSlowForTesting"));
    assertTrue(commands.containsKey("currentUser"));
    assertEquals(BigInteger.ZERO, commands.get("wait"));
  }

  @Test
  public void testDocumentation() {
    Set<Argument> requiredArgs = _manager.getRequiredArguments("wait");
    assertEquals(1, requiredArgs.size());
    assertTrue(requiredArgs.contains(new Argument("table", "String", "The name of the table.")));

    Set<Argument> optionalArgs = _manager.getOptionalArguments("wait");
    System.out.println(optionalArgs);
    assertEquals(2, optionalArgs.size());
    assertTrue(optionalArgs.contains(new Argument("seconds", "int",
        "The number of seconds to sleep, the default is 30 seconds.")));
    assertTrue(optionalArgs.contains(new Argument("shards", "Set<String>", "The shard ids (e.g. shard-0000000).")));

  }

  @Test
  public void testNewCommandLoading() throws IOException, TimeoutException, InterruptedException, ExceptionCollector {
    _manager.close();
    _tmpPath.mkdirs();
    File commandPath = new File(_commandPath);
    rmr(commandPath);
    if (commandPath.exists()) {
      fail("Command path [" + commandPath + "] still exists.");
    }
    commandPath.mkdirs();
    {
      InputStream inputStream = getClass().getResourceAsStream("/org/apache/blur/command/test1/test1.jar");
      File dest = new File(commandPath, "test1.jar");
      FileOutputStream output = new FileOutputStream(dest);
      IOUtils.copy(inputStream, output);
      inputStream.close();
      output.close();
    }
    ShardCommandManager manager = new ShardCommandManager(getIndexServer(), _tmpPath, _commandPath, 10, 10, 1000,
        _config);
    {
      BlurObject args = new BlurObject();
      args.put("table", "test");
      ArgumentOverlay argumentOverlay = new ArgumentOverlay(args, new BlurObjectSerDe());
      Response response = manager.execute(getTableContextFactory(), "test", argumentOverlay);
      Map<Shard, Object> shardResults = response.getShardResults();
      for (Object o : shardResults.values()) {
        assertEquals("test1", o);
      }
    }

    {
      InputStream inputStream = getClass().getResourceAsStream("/org/apache/blur/command/test2/test2.jar");
      File dest = new File(commandPath, "test2.jar");
      FileOutputStream output = new FileOutputStream(dest);
      IOUtils.copy(inputStream, output);
      inputStream.close();
      output.close();
    }

    assertEquals(1, manager.commandRefresh());

    {
      BlurObject args = new BlurObject();
      args.put("table", "test");
      ArgumentOverlay argumentOverlay = new ArgumentOverlay(args, new BlurObjectSerDe());
      Response response = manager.execute(getTableContextFactory(), "test", argumentOverlay);
      Map<Shard, Object> shardResults = response.getShardResults();
      for (Object o : shardResults.values()) {
        assertEquals("test2", o);
      }
    }

    // For closing.
    _manager = manager;
  }

  private void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

  @Test
  public void testShardCommandManagerNormalWait() throws IOException, TimeoutException, ExceptionCollector {
    Response response;
    Long instanceExecutionId = null;

    BlurObject args = new BlurObject();
    args.put("table", "test");
    args.put("seconds", 5);

    ArgumentOverlay argumentOverlay = new ArgumentOverlay(args, new BlurObjectSerDe());

    long start = System.nanoTime();
    while (true) {
      if (System.nanoTime() - start >= TimeUnit.SECONDS.toNanos(7)) {
        fail();
      }
      try {
        if (instanceExecutionId == null) {
          TableContextFactory tableContextFactory = getTableContextFactory();
          response = _manager.execute(tableContextFactory, "wait", argumentOverlay);
        } else {
          response = _manager.reconnect(instanceExecutionId);
        }
        break;
      } catch (TimeoutException te) {
        instanceExecutionId = te.getInstanceExecutionId();
      }
    }
    System.out.println(response);
  }

  @Test
  public void testShardCommandManagerErrorWait() throws IOException, TimeoutException, ExceptionCollector {
    BlurObject args = new BlurObject();
    args.put("table", "test");
    args.put("seconds", 1);
    ArgumentOverlay argumentOverlay = new ArgumentOverlay(args, new BlurObjectSerDe());
    TableContextFactory tableContextFactory = getTableContextFactory();
    try {
      _manager.execute(tableContextFactory, "error", argumentOverlay);
      fail();
    } catch (ExceptionCollector e) {
      Throwable t = e.getCause();
      assertTrue(t instanceof RuntimeException);
      assertEquals("error-test", t.getMessage());
    }
  }

  @Test
  public void testShardCommandManagerNormalWithCancel() throws IOException, TimeoutException, ExceptionCollector,
      BlurException, InterruptedException {

    String commandExecutionId = "TEST_COMMAND_ID1";

    BlurObjectSerDe serDe = new BlurObjectSerDe();
    WaitForSeconds waitForSeconds = new WaitForSeconds();
    waitForSeconds.setTable("test");
    waitForSeconds.setSeconds(5);
    waitForSeconds.setCommandExecutionId(commandExecutionId);

    Arguments arguments = CommandUtil.toArguments(waitForSeconds, serDe);
    BlurObject args = CommandUtil.toBlurObject(arguments);
    System.out.println(args.toString(1));
    final ArgumentOverlay argumentOverlay = new ArgumentOverlay(args, serDe);

    final AtomicBoolean fail = new AtomicBoolean();
    final AtomicBoolean running = new AtomicBoolean(true);

    new Thread(new Runnable() {
      @Override
      public void run() {
        TableContextFactory tableContextFactory = getTableContextFactory();
        Long instanceExecutionId = null;
        while (true) {
          try {
            Response response;
            if (instanceExecutionId == null) {
              response = _manager.execute(tableContextFactory, "wait", argumentOverlay);
            } else {
              response = _manager.reconnect(instanceExecutionId);
            }
            fail.set(true);
            System.out.println(response);
            return;
          } catch (IOException e) {
            if (e.getCause() instanceof CancellationException) {
              return;
            }
            e.printStackTrace();
            fail.set(true);
            return;
          } catch (TimeoutException e) {
            instanceExecutionId = e.getInstanceExecutionId();
          } catch (Exception e) {
            e.printStackTrace();
            fail.set(true);
            return;
          } finally {
            running.set(false);
          }
        }
      }
    }).start();
    Thread.sleep(1000);
    _manager.cancelCommand(commandExecutionId);
    Thread.sleep(5000);
    if (fail.get() || running.get()) {
      fail("Fail [" + fail.get() + "] Running [" + running.get() + "]");
    }
  }

  private TableContextFactory getTableContextFactory() {
    return new TableContextFactory() {
      @Override
      public TableContext getTableContext(String table) throws IOException {
        return TableContext.create(getTableDescriptor());
      }
    };
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
          String shardName = ShardUtil.getShardName(i);
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

      @Override
      public long getSegmentImportInProgressCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public long getSegmentImportPendingCount(String table) throws IOException {
        throw new RuntimeException("Not implemented.");
      }
    };
  }

  protected BlurIndex getNullBlurIndex(String shard) throws IOException {
    ShardContext shardContext = ShardContext.create(getTableContextFactory().getTableContext("test"), shard);
    return new BlurIndex(shardContext, null, null, null, null, null, null, null) {

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
      public IndexSearcherCloseable getIndexSearcher() throws IOException {
        IndexReader reader = getEmtpyReader();
        return new IndexSearcherCloseableBase(reader,null) {
          
          @Override
          public Directory getDirectory() {
            throw new RuntimeException("Not implemented.");
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

      @Override
      public void finishBulkMutate(String bulkId, boolean apply, boolean blockUntilComplete) throws IOException {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public void addBulkMutate(String bulkId, RowMutation mutation) throws IOException {
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
    };
  }

  protected IndexReader getEmtpyReader() throws IOException {
    return DirectoryReader.open(getEmtpyDirectory());
  }

  protected Directory getEmtpyDirectory() {
    return EMPTY_INDEX;
  }

}
