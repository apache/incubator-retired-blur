package org.apache.blur.command;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

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

public abstract class ClusterContext extends BaseContext {

  public abstract <T> Map<Shard, T> readIndexes(IndexRead<T> command) throws IOException;
  public abstract <T> Map<Shard, Future<T>> readIndexesAsync(IndexRead<T> command) throws IOException;

  public abstract <T> T readIndex(IndexRead<T> command) throws IOException;
  public abstract <T> Future<T> readIndexAsync(IndexRead<T> command) throws IOException;

  public abstract <T> Map<Server, T> readServers(ServerRead<?, T> command) throws IOException;
  public abstract <T> Map<Server, Future<T>> readServersAsync(ServerRead<?, T> command) throws IOException;

}
