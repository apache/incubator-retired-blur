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

import java.io.IOException;
import java.util.Map;

import org.apache.blur.command.CombiningContext;
import org.apache.blur.command.Location;

/**
 * <p>
 * The TableReadCommand is a base command over a single table for use when the
 * return type from an individual shard is the same as the return type for the
 * combine.
 * </p>
 * <p>
 * Example usage:
 * 
 * <pre>
 * public class DocFreqCommand extends TableReadCommand<Long> {
 * private static final String NAME = "docFreq";
 * 
 * @RequiredArgument
 * private String fieldName;
 * 
 * @RequiredArgument
 * private String term;
 * ...
 * 
 * @Override
 * public Long execute(IndexContext context) throws IOException {
 *   return new Long(context.getIndexReader().docFreq(new Term(fieldName, term)));
 * }
 * 
 * @Override
 * public Long combine(CombiningContext context, Iterable<Long> results) throws IOException,
 *     InterruptedException {
 *   
 *   Long total = 0l;
 *   
 *   for(Long shardTotal: results) {
 *     total += shardTotal;
 *   }
 *   
 *   return total;
 * }
 * ...
 * }
 * </pre>
 * 
 * </p>
 */
public abstract class TableReadCommand<T> extends ClusterServerReadCommandSingleTable<T> {

  public abstract T combine(CombiningContext context, Iterable<T> results) throws IOException, InterruptedException;

  @Override
  public T combine(CombiningContext context, Map<? extends Location<?>, T> results) throws IOException,
      InterruptedException {
    return combine(context, results.values());
  }

}
