package com.nearinfinity.agent.cleaners;

import org.junit.Before;
import org.junit.Test;
import com.nearinfinity.BlurAgentBaseTestClass;
import com.nearinfinity.agent.connections.cleaners.interfaces.CleanerDatabaseInterface;

public class QueryCleanerTest extends BlurAgentBaseTestClass {
  private CleanerDatabaseInterface database;
  
  @Before
  public void setup() {
    this.database = new MockCleanerDatabaseConnection();
  }
  
  @Test
  public void shouldExpireAndDeleteOldQueries() {
    // TableCollector.startCollecting(MiniCluster.getControllerConnectionStr(), "test", jdbc);
  }
}