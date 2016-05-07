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
package org.apache.blur.command;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.blur.thrift.generated.CommandStatus;
import org.apache.blur.thrift.generated.CommandStatusState;

public class CommandStatusUtil {

  public static CommandStatus mergeCommandStatus(CommandStatus cs1, CommandStatus cs2) {
    if (cs1 == null && cs2 == null) {
      return null;
    } else if (cs1 == null) {
      return cs2;
    } else if (cs2 == null) {
      return cs1;
    } else {
      Map<String, Map<CommandStatusState, Long>> serverStateMap1 = cs1.getServerStateMap();
      Map<String, Map<CommandStatusState, Long>> serverStateMap2 = cs2.getServerStateMap();
      Map<String, Map<CommandStatusState, Long>> merge = mergeServerStateMap(serverStateMap1, serverStateMap2);
      return new CommandStatus(cs1.getExecutionId(), cs1.getCommandName(), cs1.getArguments(), merge, cs1.getUser());
    }
  }

  private static Map<String, Map<CommandStatusState, Long>> mergeServerStateMap(
      Map<String, Map<CommandStatusState, Long>> serverStateMap1,
      Map<String, Map<CommandStatusState, Long>> serverStateMap2) {
    Map<String, Map<CommandStatusState, Long>> result = new HashMap<String, Map<CommandStatusState, Long>>();
    Set<String> keys = new HashSet<String>();
    keys.addAll(serverStateMap1.keySet());
    keys.addAll(serverStateMap2.keySet());
    for (String key : keys) {
      Map<CommandStatusState, Long> css1 = serverStateMap1.get(key);
      Map<CommandStatusState, Long> css2 = serverStateMap2.get(key);
      result.put(key, mergeCommandStatusState(css1, css2));
    }
    return result;
  }

  private static Map<CommandStatusState, Long> mergeCommandStatusState(Map<CommandStatusState, Long> css1,
      Map<CommandStatusState, Long> css2) {
    if (css1 == null && css2 == null) {
      return new HashMap<CommandStatusState, Long>();
    } else if (css1 == null) {
      return css2;
    } else if (css2 == null) {
      return css1;
    } else {
      Map<CommandStatusState, Long> result = new HashMap<CommandStatusState, Long>(css1);
      for (Entry<CommandStatusState, Long> e : css2.entrySet()) {
        CommandStatusState key = e.getKey();
        Long l = result.get(key);
        Long value = e.getValue();
        if (l == null) {
          result.put(key, value);
        } else {
          result.put(key, l + value);
        }
      }
      return result;
    }
  }
}
