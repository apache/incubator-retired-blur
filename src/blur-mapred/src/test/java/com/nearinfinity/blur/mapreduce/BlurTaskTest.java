package com.nearinfinity.blur.mapreduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.nearinfinity.blur.thrift.generated.TableDescriptor;

import static org.junit.Assert.*;

public class BlurTaskTest {

  @Test
  public void testGetNumReducersBadPath() {
    BlurTask task = new BlurTask();
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setShardCount(5);
    tableDescriptor.setTableUri("file:///tmp/blur34746545");
    tableDescriptor.setName("blur34746545");
    task.setTableDescriptor(tableDescriptor);
    assertEquals(5, task.getNumReducers(new Configuration()));
  }

  @Test
  public void testGetNumReducersValidPath() {
    new File("/tmp/blurTestShards/shard-1/").mkdirs();
    new File("/tmp/blurTestShards/shard-2/").mkdirs();
    new File("/tmp/blurTestShards/shard-3/").mkdirs();
    try {
      BlurTask task = new BlurTask();
      TableDescriptor tableDescriptor = new TableDescriptor();
      tableDescriptor.setShardCount(5);
      tableDescriptor.setTableUri("file:///tmp/blurTestShards");
      tableDescriptor.setName("blurTestShards");
      task.setTableDescriptor(tableDescriptor);
      assertEquals(3, task.getNumReducers(new Configuration()));
    } finally {
      new File("/tmp/blurTestShards/shard-1/").delete();
      new File("/tmp/blurTestShards/shard-2/").delete();
      new File("/tmp/blurTestShards/shard-3/").delete();
      new File("/tmp/blurTestShards/").delete();
    }
  }
}
