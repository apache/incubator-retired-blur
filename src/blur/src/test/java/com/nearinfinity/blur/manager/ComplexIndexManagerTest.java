package com.nearinfinity.blur.manager;

import static com.nearinfinity.blur.manager.IndexManagerTest.rm;

import java.io.File;

import junit.framework.TestCase;

import com.nearinfinity.mele.Mele;

public class ComplexIndexManagerTest extends TestCase {

	private Mele mele;

	@Override
	protected void setUp() throws Exception {
		rm(new File("target/test-tmp"));
		mele = Mele.getMele(new LocalHdfsMeleConfiguration());
		mele.createDirectoryCluster("complex-test");
		mele.createDirectory("complex-test", "shard");
		populate();
	}

	private void populate() {
		
	}
	
	public void testValue() {}

	
	
	
}
