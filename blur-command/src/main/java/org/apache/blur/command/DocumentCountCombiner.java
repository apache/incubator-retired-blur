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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.command.Command;
import org.apache.blur.command.ClusterCommand;
import org.apache.blur.command.ClusterContext;
import org.apache.blur.command.IndexContext;
import org.apache.blur.command.IndexReadCombiningCommand;
import org.apache.blur.command.Server;
import org.apache.blur.command.Shard;

@SuppressWarnings("serial")
public class DocumentCountCombiner extends Command implements ClusterCommand<Long>,
    IndexReadCombiningCommand<Integer, Long> {

  private static final String DOC_COUNT_AGGREGATE = "docCountAggregate";

  @Override
  public String getName() {
    return DOC_COUNT_AGGREGATE;
  }

  @Override
  public Integer execute(IndexContext context) throws IOException {
    return context.getIndexReader().numDocs();
  }

  @Override
  public Long combine(Map<Shard, Integer> results) throws IOException {
    long total = 0;
    for (Integer i : results.values()) {
      total += i;
    }
    return total;
  }

  @Override
  public Long clusterExecute(ClusterContext context) throws IOException {
    Map<Server, Long> results = context.readServers(null, DocumentCountCombiner.class);
    long total = 0;
    for (Entry<Server, Long> e : results.entrySet()) {
      total += e.getValue();
    }
    return total;
  }

}
