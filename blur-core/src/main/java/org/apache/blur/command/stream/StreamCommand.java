/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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