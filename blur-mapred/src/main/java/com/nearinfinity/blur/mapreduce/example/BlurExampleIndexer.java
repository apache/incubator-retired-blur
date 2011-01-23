package com.nearinfinity.blur.mapreduce.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.nearinfinity.blur.mapreduce.BlurMapper;
import com.nearinfinity.blur.mapreduce.BlurRecord;
import com.nearinfinity.blur.mapreduce.BlurReducer;
import com.nearinfinity.blur.mapreduce.BlurTask;

public class BlurExampleIndexer {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: blurindexer <in> <out>");
            System.exit(2);
        }
        
        BlurTask blurTask = new BlurTask(configuration);
        blurTask.setBlurAnalyzerStr("");
        blurTask.setCommitPointToOpen("EMPTY");
        blurTask.setNewCommitPoint("new-test");
        blurTask.setTableName("test");
        blurTask.setBasePath("./blur-testing");
        
        Job job = new Job(configuration, "Blur Indexer");
        job.setJarByClass(BlurExampleIndexer.class);
        job.setMapperClass(BlurMapper.class);
        job.setReducerClass(BlurReducer.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BlurRecord.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1],"job-" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
