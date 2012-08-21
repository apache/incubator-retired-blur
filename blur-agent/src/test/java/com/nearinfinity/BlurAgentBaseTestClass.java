package com.nearinfinity;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.nearinfinity.blur.MiniCluster;

public abstract class BlurAgentBaseTestClass {
	@BeforeClass
	public static void setupBlur() {
		MiniCluster.startDfs("./tmp");
		MiniCluster.startZooKeeper("./tmp");
		
		MiniCluster.startControllers(1);
		MiniCluster.startShards(1);
	}
	
	@AfterClass
	public static void tearDownBlur() {
		MiniCluster.stopShards();
		MiniCluster.stopControllers();
		
		MiniCluster.shutdownZooKeeper();
		MiniCluster.shutdownDfs();
	}
}
