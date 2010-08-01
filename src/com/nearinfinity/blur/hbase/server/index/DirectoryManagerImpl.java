package com.nearinfinity.blur.hbase.server.index;

import java.util.Map;

import org.apache.lucene.store.Directory;
import org.apache.zookeeper.ZooKeeper;


public class DirectoryManagerImpl implements DirectoryManager {

	public DirectoryManagerImpl(ZooKeeper zk) {

	}

	@Override
	public Map<String, Directory> getCurrentDirectories() {
		return null;
	}
	
	//this.dir = new ZookeeperWrapperDirectory(zk, FSDirectory.open(new File("/Users/amccurry/testIndex")),"/blur/Users/amccurry/testIndex");

}
