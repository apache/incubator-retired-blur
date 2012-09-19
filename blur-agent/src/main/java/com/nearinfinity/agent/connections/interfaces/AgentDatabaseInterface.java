package com.nearinfinity.agent.connections.interfaces;

public interface AgentDatabaseInterface {
  String getConnectionString(String zookeeperName);
  void setHdfsInfo(String name, String host, int port);
}
