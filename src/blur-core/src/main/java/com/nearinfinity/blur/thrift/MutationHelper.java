package com.nearinfinity.blur.thrift;

import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import com.nearinfinity.blur.BlurShardName;
import com.nearinfinity.blur.manager.BlurPartitioner;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.utils.BlurConstants;

public class MutationHelper {

  public static String getShardName(String table, String rowId, int numberOfShards, BlurPartitioner<BytesWritable, ?> blurPartitioner) {
    BytesWritable key = getKey(rowId);
    int partition = blurPartitioner.getPartition(key, null, numberOfShards);
    return BlurShardName.getShardName(BlurConstants.SHARD_PREFIX, partition);
  }

  public static void validateMutation(RowMutation mutation) {
    if (mutation == null) {
      throw new NullPointerException("Mutation can not be null.");
    }
    if (mutation.rowId == null) {
      throw new NullPointerException("Rowid can not be null in mutation.");
    }
    if (mutation.table == null) {
      throw new NullPointerException("Table can not be null in mutation.");
    }
  }

  public static BytesWritable getKey(String rowId) {
    return new BytesWritable(rowId.getBytes());
  }

  public static Row getRowFromMutations(String id, List<RecordMutation> recordMutations) {
    Row row = new Row().setId(id);
    for (RecordMutation mutation : recordMutations) {
      Record record = mutation.getRecord();
      switch (mutation.recordMutationType) {
      case REPLACE_ENTIRE_RECORD:
        row.addToRecords(record);
        break;
      default:
        throw new RuntimeException("Not supported [" + mutation.recordMutationType + "]");
      }
    }
    return row;
  }
}
