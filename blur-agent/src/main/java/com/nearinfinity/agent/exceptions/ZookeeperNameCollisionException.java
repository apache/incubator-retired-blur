package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class ZookeeperNameCollisionException extends CollisionException {
  public ZookeeperNameCollisionException(int size, String zookeeperName)
  {
    super(size, "Zookeepers", zookeeperName);
  }
}
