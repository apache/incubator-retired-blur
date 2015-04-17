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
package org.apache.blur.mapreduce.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.blur.mapreduce.lib.CsvBlurDriver.ControllerPool;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

public class CsvBlurDriverTest {

  protected Path root = new Path("./target/tmp/CsvBlurDriverTest/");
  protected int shardCount = 13;
  protected Path _path1;
  protected Path _path2;

  @Before
  public void setup() throws IOException {
    Configuration configuration = new Configuration();
    _path1 = new Path(root, "test1");
    _path2 = new Path(root, "test2");
    FileSystem fileSystem = _path1.getFileSystem(configuration);
    fileSystem.mkdirs(_path1);
    fileSystem.mkdirs(_path2);
  }

  @Test
  public void testCsvBlurDriverTestFail1() throws Exception {
    Configuration configuration = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return null;
      }
    };
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    assertNull(CsvBlurDriver.setupJob(configuration, controllerPool, ref, new String[] {}));
  }

  @Test
  public void testCsvBlurDriverTest() throws Exception {
    Configuration configurationSetup = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return getMockIface();
      }
    };
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, ref, "-c", "host:40010", "-d", "family1",
        "col1", "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", _path1.toString(), "-i",
        _path2.toString());
    assertNotNull(job);
    Configuration configuration = job.getConfiguration();
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    assertEquals(tableDescriptor.getName(), "table1");
    Collection<String> inputs = configuration.getStringCollection("mapred.input.dir");
    assertEquals(2, inputs.size());
    Map<String, List<String>> familyAndColumnNameMap = CsvBlurMapper.getFamilyAndColumnNameMap(configuration);
    assertEquals(2, familyAndColumnNameMap.size());
  }

  @Test
  public void testCsvBlurDriverTest2() throws Exception {
    Configuration configurationSetup = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return getMockIface();
      }
    };
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, ref, "-c", "host:40010", "-d", "family1",
        "col1", "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", _path1.toString(), "-i",
        _path2.toString(), "-S", "-C", "1000000", "2000000");
    assertNotNull(job);
    Configuration configuration = job.getConfiguration();
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    assertEquals(tableDescriptor.getName(), "table1");
    Collection<String> inputs = configuration.getStringCollection("mapred.input.dir");
    assertEquals(2, inputs.size());
    Map<String, List<String>> familyAndColumnNameMap = CsvBlurMapper.getFamilyAndColumnNameMap(configuration);
    assertEquals(2, familyAndColumnNameMap.size());
  }

  @Test
  public void testCsvBlurDriverTest3() throws Exception {
    Configuration configurationSetup = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return getMockIface();
      }
    };
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, ref, "-c", "host:40010", "-d", "family1",
        "col1", "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", _path1.toString(), "-i",
        _path2.toString(), "-S", "-C", "1000000", "2000000", "-p", "SNAPPY");
    assertNotNull(job);
    Configuration configuration = job.getConfiguration();
    TableDescriptor tableDescriptor = BlurOutputFormat.getTableDescriptor(configuration);
    assertEquals(tableDescriptor.getName(), "table1");
    Collection<String> inputs = configuration.getStringCollection("mapred.input.dir");
    assertEquals(2, inputs.size());
    Map<String, List<String>> familyAndColumnNameMap = CsvBlurMapper.getFamilyAndColumnNameMap(configuration);
    assertEquals(2, familyAndColumnNameMap.size());
    assertEquals("true", configuration.get(CsvBlurDriver.MAPRED_COMPRESS_MAP_OUTPUT));
    assertEquals(SnappyCodec.class.getName(), configuration.get(CsvBlurDriver.MAPRED_MAP_OUTPUT_COMPRESSION_CODEC));
  }

  @Test
  public void multiplierParamShouldIncreaseReduceTasks() throws Exception {
    Configuration configurationSetup = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return getMockIface();
      }
    };
    int multiplierParam = 10;
    AtomicReference<Callable<Void>> ref = new AtomicReference<Callable<Void>>();
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, ref, "-c", "host:40010", "-d", "family1",
        "col1", "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", _path1.toString(), "-i",
        _path2.toString(), "-S", "-C", "1000000", "2000000", "-p", "SNAPPY", "-r", Integer.toString(multiplierParam));
    assertNotNull(job);

    assertEquals(multiplierParam * shardCount, job.getNumReduceTasks());
  }

  protected Iface getMockIface() {
    InvocationHandler handler = new InvocationHandler() {

      @Override
      public Object invoke(Object o, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("describe")) {
          TableDescriptor tableDescriptor = new TableDescriptor();
          tableDescriptor.setName((String) args[0]);
          tableDescriptor.setTableUri(new Path(root, "tmppath").toString());
          tableDescriptor.setShardCount(shardCount);
          return tableDescriptor;
        }
        throw new RuntimeException("not implemented.");
      }
    };
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class }, handler);
  }

}
