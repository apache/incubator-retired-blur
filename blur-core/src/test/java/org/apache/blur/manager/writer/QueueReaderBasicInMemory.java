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

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.blur.server.ShardContext;
import org.apache.blur.thrift.generated.RowMutation;

public class QueueReaderBasicInMemory extends QueueReader {

  static final BlockingQueue<RowMutation> _queue = new ArrayBlockingQueue<RowMutation>(1000);

  public QueueReaderBasicInMemory(BlurIndex index, ShardContext shardContext) {
    super(index, shardContext);
  }

  @Override
  public void take(List<RowMutation> mutations, int max) {
    _queue.drainTo(mutations, max);
  }

  @Override
  public void success() {

  }

  @Override
  public void failure() {

  }

}
