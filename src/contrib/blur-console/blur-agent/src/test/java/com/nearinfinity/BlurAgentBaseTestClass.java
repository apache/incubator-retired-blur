package com.nearinfinity;

import org.apache.blur.MiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class BlurAgentBaseTestClass extends AgentBaseTestClass {
	@BeforeClass
	public static void startBlur() {
		MiniCluster.startDfs("./tmp");
		MiniCluster.startZooKeeper("./tmp");
		MiniCluster.startControllers(1);
		MiniCluster.startShards(1);
	}

	@AfterClass
	public static void stopBlur() {
		MiniCluster.stopShards();
		MiniCluster.stopControllers();
		MiniCluster.shutdownZooKeeper();
		MiniCluster.shutdownDfs();
	}
}
