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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.blur.thrift.generated.Blur.Iface;

public abstract class Command<R> implements Cloneable {

  public abstract String getName();

  public abstract R run() throws IOException;

  public abstract R run(String connectionStr) throws IOException;

  public abstract R run(Iface client) throws IOException;

  public abstract Set<String> routeTables(BaseContext context);

  public abstract Set<Shard> routeShards(BaseContext context, Set<String> tables);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Command<R> clone() {
    try {
      return (Command) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static <T> Set<T> asSet(T t) {
    Set<T> set = new HashSet<T>();
    set.add(t);
    return set;
  }

}
