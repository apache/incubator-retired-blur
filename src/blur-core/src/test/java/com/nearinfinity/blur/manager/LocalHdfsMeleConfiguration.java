package com.nearinfinity.blur.manager;

import java.io.IOException;
import java.util.Arrays;

import com.nearinfinity.mele.MeleConfiguration;

public class LocalHdfsMeleConfiguration extends MeleConfiguration {
	
    private static final long serialVersionUID = 929950426507059728L;

    public LocalHdfsMeleConfiguration(String baseDir) throws IOException {
		this.setBaseHdfsPath(baseDir + "/mele-hdfs");
		this.setLocalReplicationPathList(Arrays.asList(baseDir + "/mele1",baseDir + "/mele2",baseDir + "/mele3"));
	}

}
