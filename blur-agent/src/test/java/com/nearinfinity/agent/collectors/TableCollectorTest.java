package com.nearinfinity.agent.collectors;

import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.BlurAgentBaseTestClass;
import com.nearinfinity.MockDatasource;
import com.nearinfinity.agent.collectors.blur.table.TableCollector;
import com.nearinfinity.blur.MiniCluster;

public class TableCollectorTest extends BlurAgentBaseTestClass {
	private JdbcTemplate jdbc;
	
	@Before
	public void setup() {
		jdbc = new JdbcTemplate(new MockDatasource());
	}
	
	@Test
	public void testCollector() {
		TableCollector.startCollecting(MiniCluster.getControllerConnectionStr(), "test", jdbc);
	}
	
}
