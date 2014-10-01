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
package org.apache.blur.command.commandtype;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.command.BaseContext;
import org.apache.blur.command.Shard;
import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.annotation.RequiredArgument;

public abstract class ServerReadCommandSingleTable<T1, T2> extends ServerReadCommand<T1, T2> {

  @RequiredArgument("The name of the table.")
  private String table;

  @OptionalArgument("The shard ids (e.g. shard-0000000).")
  private Set<String> shards;

  @Override
  public Set<String> routeTables(BaseContext context) {
    return asSet(table);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<Shard> routeShards(BaseContext context, Set<String> tables) {
    if (shards == null) {
      return Collections.EMPTY_SET;
    }
    Set<Shard> result = new TreeSet<Shard>();
    for (String shard : shards) {
      result.add(new Shard(table, shard));
    }
    return result;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public Set<String> getShards() {
    return shards;
  }

  public void setShards(Set<String> shards) {
    this.shards = shards;
  }

  public void addShard(String shard) {
    if (shards == null) {
      shards = new TreeSet<String>();
    }
    shards.add(shard);
  }

}
