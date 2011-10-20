package com.nearinfinity.blur.mapreduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class BlurTaskTest {

  @Test
  public void testGetNumReducersBadPath() {
    BlurTask task = new BlurTask();
//    task.setBasePath("file:///tmp/");
//    task.setTableName("blur34746545");
//    task.setNumReducers(5);
    assertEquals(5, task.getNumReducers(new Configuration()));
  }

  @Test
  public void testGetNumReducersValidPath() {
    new File("/tmp/blurTestShards/shard1/").mkdirs();
    new File("/tmp/blurTestShards/shard2/").mkdirs();
    new File("/tmp/blurTestShards/shard3/").mkdirs();
    try {
      BlurTask task = new BlurTask();
//      task.setBasePath("file:///tmp/");
//      task.setTableName("blurTestShards");
//      task.setNumReducers(5);
      assertEquals(3, task.getNumReducers(new Configuration()));
    } finally {
      new File("/tmp/blurTestShards/shard1/").delete();
      new File("/tmp/blurTestShards/shard2/").delete();
      new File("/tmp/blurTestShards/shard3/").delete();
      new File("/tmp/blurTestShards/").delete();
    }
  }
}
