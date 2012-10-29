package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class CollisionException extends Exception {
  public CollisionException(int size, String type, String name)
  {
    super("Found [" + size + "] " + type + " by identifier [" + name + "].  Need one and only one result.  Skipping collection.");
  }
}
