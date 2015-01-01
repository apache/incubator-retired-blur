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

import org.apache.blur.command.annotation.Description;
import org.apache.blur.command.commandtype.ClusterServerReadCommandSingleTable;

@Description("Gets the number of visible documents in the index.")
public class DocumentCountDefaultClusterCombine extends ClusterServerReadCommandSingleTable<Long> {

  private static final String DOC_COUNT_CLUSTER_COMBINE = "docCountClusterCombine";

  @Override
  public String getName() {
    return DOC_COUNT_CLUSTER_COMBINE;
  }

  @Override
  public Long execute(IndexContext context) throws IOException {
    return (long) context.getIndexReader().numDocs();
  }

  @Override
  public Long combine(CombiningContext context, Map<? extends Location<?>, Long> results) throws IOException {
    long total = 0;
    for (Long l : results.values()) {
      total += l;
    }
    return total;
  }

}
