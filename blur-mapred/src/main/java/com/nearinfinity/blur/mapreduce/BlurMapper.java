package com.nearinfinity.blur.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public abstract class BlurMapper extends Mapper<LongWritable, Text, BytesWritable, BlurRecord> {

    protected BlurRecord record;
    protected BytesWritable key;
    protected BlurTask blurTask;
    protected Counter recordCounter;
    protected Counter fieldCounter;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        long maxRecordCount = blurTask.getMaxRecordCount();
        if (maxRecordCount == -1) {
            maxRecordCount = Long.MAX_VALUE;
        }
        for (long l = 0; l < maxRecordCount && context.nextKeyValue(); l++) {
            map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
        cleanup(context);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        blurTask = new BlurTask(context);
        record = new BlurRecord();
        key = new BytesWritable();
        recordCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRecordCounterName());
        fieldCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getFieldCounterName());
    }

    @Override
    protected abstract void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException;

}
