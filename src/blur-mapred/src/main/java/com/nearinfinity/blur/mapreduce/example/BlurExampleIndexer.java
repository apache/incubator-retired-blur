/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.mapreduce.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
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
        
        BlurAnalyzer blurAnalyzer = new BlurAnalyzer(new StandardAnalyzer(Version.LUCENE_33));
        BlurTask blurTask = new BlurTask(configuration);
        blurTask.setBlurAnalyzer(blurAnalyzer);
        blurTask.setTableName("test");
        blurTask.setBasePath("./blur-testing");
        
        Job job = new Job(configuration, "Blur Indexer");
        job.setJarByClass(BlurExampleIndexer.class);
        job.setMapperClass(BlurExampleMapper.class);
        job.setReducerClass(BlurReducer.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(BlurRecord.class);
        job.setNumReduceTasks(blurTask.getNumReducers(10));
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1],"job-" + System.currentTimeMillis()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
