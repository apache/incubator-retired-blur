package org.apache.blur.mapreduce.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.blur.mapreduce.lib.CsvBlurDriver.ControllerPool;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class CsvBlurDriverTest {

  protected String tableUri = "file:///tmp/tmppath";
  protected int shardCount = 13;

  @Test
  public void testCsvBlurDriverTestFail1() throws Exception {
    Configuration configuration = new Configuration();
    ControllerPool controllerPool = new CsvBlurDriver.ControllerPool() {
      @Override
      public Iface getClient(String controllerConnectionStr) {
        return null;
      }
    };
    assertNull(CsvBlurDriver.setupJob(configuration, controllerPool, new String[] {}));
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
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, "-c", "host:40010", "-d", "family1", "col1",
        "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", "file:///tmp/test1", "-i", "file:///tmp/test2");
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
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, "-c", "host:40010", "-d", "family1", "col1",
        "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", "file:///tmp/test1", "-i", "file:///tmp/test2",
        "-S", "-C", "1000000", "2000000");
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
    Job job = CsvBlurDriver.setupJob(configurationSetup, controllerPool, "-c", "host:40010", "-d", "family1", "col1",
        "col2", "-d", "family2", "col3", "col4", "-t", "table1", "-i", "file:///tmp/test1", "-i", "file:///tmp/test2",
        "-S", "-C", "1000000", "2000000", "-p", "SNAPPY");
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

  protected Iface getMockIface() {
    InvocationHandler handler = new InvocationHandler() {

      @Override
      public Object invoke(Object o, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("describe")) {
          TableDescriptor tableDescriptor = new TableDescriptor();
          tableDescriptor.setName((String) args[0]);
          tableDescriptor.setTableUri(tableUri);
          tableDescriptor.setShardCount(shardCount);
          return tableDescriptor;
        }
        throw new RuntimeException("not implemented.");
      }
    };
    return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class }, handler);
  }

}
