package com.nearinfinity.blur.manager.stats;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.utils.BlurExecutorCompletionService;
import com.nearinfinity.blur.utils.ForkJoin.Merger;

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
    s1.queries = Math.max(s1.queries, s2.queries);
    return s1;
  }

}
