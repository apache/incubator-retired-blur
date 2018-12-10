package org.apache.blur.manager.stats;

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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.TableStats;
import org.apache.blur.utils.BlurExecutorCompletionService;
import org.apache.blur.utils.ForkJoin.Merger;


public class MergerTableStats implements Merger<TableStats> {

  private long _timeout;

  public MergerTableStats(long timeout) {
    _timeout = timeout;
  }

  @Override
  public TableStats merge(BlurExecutorCompletionService<TableStats> service) throws BlurException {
    TableStats result = new TableStats();
    while (service.getRemainingCount() > 0) {
      Future<TableStats> tableStats = service.poll(_timeout, TimeUnit.MILLISECONDS, true);
      TableStats stats = service.getResultThrowException(tableStats);
      result = merge(result, stats);
    }
    return result;
  }

  private TableStats merge(TableStats s1, TableStats s2) {
    s1.tableName = s2.tableName;
    s1.bytes = Math.max(s1.bytes, s2.bytes);
    s1.recordCount = s1.recordCount + s2.recordCount;
    s1.rowCount = s1.rowCount + s2.rowCount;
    s1.segmentImportInProgressCount = s1.segmentImportInProgressCount + s2.segmentImportInProgressCount;
    s1.segmentImportPendingCount = s1.segmentImportPendingCount + s2.segmentImportPendingCount;
    return s1;
  }

}
