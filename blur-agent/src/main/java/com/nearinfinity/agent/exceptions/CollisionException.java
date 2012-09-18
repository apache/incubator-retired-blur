package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class CollisionException extends Exception {
  private String error;
  
  public CollisionException(int size, String type, String name)
  {
    super();
    this.error = "Found [" + size + "] " + type + " by identifier [" + name + "].  Need one and only one result.  Skipping collection.";
  }
  
  public String getError()
  {
    return this.error;
  }
}
