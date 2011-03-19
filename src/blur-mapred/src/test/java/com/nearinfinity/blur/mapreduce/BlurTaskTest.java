package com.nearinfinity.blur.mapreduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class BlurTaskTest {

	@Test
	public void testGetNumReducersBadPath() {
		BlurTask task = new BlurTask(new Configuration());
		task.setBasePath("file:///tmp/");
		task.setTableName("blur34746545");
		assertEquals(5, task.getNumReducers(5));
	}

	@Test
	public void testGetNumReducersValidPath() {
		new File("/tmp/blurTestShards/shard1/").mkdirs();
		new File("/tmp/blurTestShards/shard2/").mkdirs();
		new File("/tmp/blurTestShards/shard3/").mkdirs();
		try {
			BlurTask task = new BlurTask(new Configuration());
			task.setBasePath("file:///tmp/");
			task.setTableName("blurTestShards");
			assertEquals(3, task.getNumReducers(5));
		} finally {
			new File("/tmp/blurTestShards/shard1/").delete();
			new File("/tmp/blurTestShards/shard2/").delete();
			new File("/tmp/blurTestShards/shard3/").delete();
			new File("/tmp/blurTestShards/").delete();
		}
	}
}
