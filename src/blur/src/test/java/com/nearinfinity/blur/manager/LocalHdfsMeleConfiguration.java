package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.nearinfinity.mele.MeleConfiguration;

public class LocalHdfsMeleConfiguration extends MeleConfiguration {
	
	public LocalHdfsMeleConfiguration(String baseDir) throws IOException {
		this.setBaseHdfsPath(baseDir + "/mele-hdfs");
		this.setLocalReplicationPathList(Arrays.asList(baseDir + "/mele1",baseDir + "/mele2",baseDir + "/mele3"));
		FileSystem fs = FileSystem.get(new Configuration());
		this.setHdfsFileSystem(fs);
	}

}
