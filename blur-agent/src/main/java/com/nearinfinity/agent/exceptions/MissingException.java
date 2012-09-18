package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class MissingException extends Exception {
  private String error;
  
  public MissingException(String type, String name)
  {
    super();
    this.error = "Couldn't Find a " + type + " by name [" + name + "].  Need a single result.  Skipping collection.";
  }
  
  public String getError()
  {
    return this.error;
  }
}
