package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.nearinfinity.mele.MeleConfiguration;

public class LocalHdfsMeleConfiguration extends MeleConfiguration {
	
	public LocalHdfsMeleConfiguration() throws IOException {
		this.setBaseHdfsPath("target/test-tmp/mele-hdfs");
		this.setLocalReplicationPathList(Arrays.asList("target/test-tmp/mele1","target/test-tmp/mele2","target/test-tmp/mele3"));
		FileSystem fs = FileSystem.get(new Configuration());
		this.setHdfsFileSystem(fs);
	}

}
