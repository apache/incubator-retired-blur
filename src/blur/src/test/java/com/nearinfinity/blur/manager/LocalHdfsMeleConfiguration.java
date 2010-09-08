package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.nearinfinity.mele.store.MeleConfiguration;

public class LocalHdfsMeleConfiguration extends MeleConfiguration {
	
	public LocalHdfsMeleConfiguration() throws IOException {
		this.setUsingHdfs(true);
		this.setBaseHdfsPath("./tmp/mele-hdfs");
		this.setLocalReplicationPathList(Arrays.asList("./tmp/mele1","./tmp/mele2","./tmp/mele3"));
		FileSystem fs = FileSystem.get(new Configuration());
		this.setHdfsFileSystem(fs);
	}

}
