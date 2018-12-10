package org.apache.blur.command.commandtype;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.blur.command.BaseContext;
import org.apache.blur.command.ClusterContext;
import org.apache.blur.command.Shard;
import org.apache.blur.command.annotation.RequiredArgument;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public abstract class ClusterExecuteCommandSingleTable<T> extends ClusterExecuteCommand<T> {

  public abstract T clusterExecute(ClusterContext context) throws IOException, InterruptedException;

  @RequiredArgument("The name of the table.")
  private String table;

  @Override
  public Set<String> routeTables(BaseContext context) {
    return asSet(table);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<Shard> routeShards(BaseContext context, Set<String> tables) {
    return Collections.EMPTY_SET;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

}
