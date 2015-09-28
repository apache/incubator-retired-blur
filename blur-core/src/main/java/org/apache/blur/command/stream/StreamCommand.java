package org.apache.blur.command.stream;

public enum StreamCommand {
  STREAM(1), CLASS_LOAD_CHECK(2), CLASS_LOAD(3), CLOSE(-1);

  private final int _command;

  private StreamCommand(int command) {
    _command = command;
  }

  public int getCommand() {
    return _command;
  }

  public static StreamCommand find(int command) {
    switch (command) {
    case -1:
      return CLOSE;
    case 1:
      return STREAM;
    case 2:
      return CLASS_LOAD_CHECK;
    case 3:
      return CLASS_LOAD;
    default:
      throw new RuntimeException("Command [" + command + "] not found.");
    }
  }
}