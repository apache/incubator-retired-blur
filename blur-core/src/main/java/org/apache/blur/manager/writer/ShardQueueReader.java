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
package org.apache.blur.manager.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.RowMutation;

public abstract class ShardQueueReader extends BaseQueueReader implements Closeable {

  private static final Log LOG = LogFactory.getLog(ShardQueueReader.class);

  private final BlurIndex _index;
  private final ShardContext _shardContext;
  private final TableContext _tableContext;

  public ShardQueueReader(BlurIndex index, ShardContext shardContext) {
    super(shardContext.getTableContext().getBlurConfiguration(), getContext(shardContext));
    _index = index;
    _shardContext = shardContext;
    _tableContext = shardContext.getTableContext();
  }

  protected void doMutate(List<RowMutation> mutations) {
    MutatableAction mutatableAction = new MutatableAction(_shardContext);
    mutatableAction.mutate(mutations);
    try {
      _index.process(mutatableAction);
      success();
    } catch (IOException e) {
      failure();
      LOG.error("Unknown error during loading of rowmutations from queue [{0}] into table [{1}] and shard [{2}].",
          this.toString(), _tableContext.getTable(), _shardContext.getShard());
    } finally {
      mutations.clear();
    }
  }

  private static String getContext(ShardContext shardContext) {
    return shardContext.getTableContext() + "/" + shardContext.getShard();
  }

}
