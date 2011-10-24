package com.nearinfinity.blur.manager;

import org.apache.hadoop.mapreduce.Partitioner;

public class BlurPartitioner<BytesWritable, V> extends Partitioner<BytesWritable, V> {

  public int getPartition(BytesWritable key, V value, int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

}
