package com.nearinfinity.blur.mapreduce;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public class BlurMapper extends Mapper<LongWritable,Text,BytesWritable,BlurRecord> {
    
    private BlurRecord record;
    private BytesWritable key;
    private BlurTask blurTask;
    private Counter recordCounter;
    private Counter fieldCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        blurTask = new BlurTask(context);
        record = new BlurRecord();
        key = new BytesWritable();
        recordCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getRecordCounterName());
        fieldCounter = context.getCounter(blurTask.getCounterGroupName(), blurTask.getFieldCounterName());
    }

    @Override
    protected void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
        record.clearColumns();
        String str = value.toString();
        String[] split = str.split("\\t");
        record.setId(UUID.randomUUID().toString());
        record.setSuperKey(UUID.randomUUID().toString());
        record.setColumnFamily("cf1");
        for (int i = 0; i < split.length; i++) {
            record.addColumn("c"+i,split[i]);
            fieldCounter.increment(1);
        }
        byte[] bs = record.getId().getBytes();
        key.set(bs, 0, bs.length);
        context.write(key, record);
        recordCounter.increment(1);
        context.progress();
    }
}
