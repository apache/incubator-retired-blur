package com.nearinfinity.agent.mailer;

public interface AgentMailerInterface {
  void notifyZookeeperOffline(String zookeeperName);
  void notifyControllerOffline(String controllerName);
  void notifyShardOffline(String shardName);
}
