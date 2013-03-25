package org.apache.blur.mapreduce.lib;

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
import org.apache.blur.mapreduce.BlurRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;


public abstract class BlurInputFormat extends InputFormat<Text, BlurRecord> {

//  @SuppressWarnings("unchecked")
//  @Override
//  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
//    List<?> splits = new ArrayList<Object>();
//    Path[] paths = FileInputFormat.getInputPaths(context);
//    for (Path path : paths) {
//      findAllSegments(context.getConfiguration(), path, splits);
//    }
//    return (List<InputSplit>) splits;
//  }
//
//  @Override
//  public RecordReader<Text, BlurRecord> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//    BlurRecordReader blurRecordReader = new BlurRecordReader();
//    blurRecordReader.initialize(split, context);
//    return blurRecordReader;
//  }
//
//  public static void findAllSegments(Configuration configuration, Path path, List<?> splits) throws IOException {
//    FileSystem fileSystem = path.getFileSystem(configuration);
//    if (fileSystem.isFile(path)) {
//      return;
//    } else {
//      FileStatus[] listStatus = fileSystem.listStatus(path);
//      for (FileStatus status : listStatus) {
//        Path p = status.getPath();
//        HdfsDirectory directory = new HdfsDirectory(p);
//        if (IndexReader.indexExists(directory)) {
//          addSplits(directory, splits);
//        } else {
//          findAllSegments(configuration, p, splits);
//        }
//      }
//    }
//  }
//
//  @SuppressWarnings("unchecked")
//  public static void addSplits(HdfsDirectory directory, @SuppressWarnings("rawtypes") List splits) throws IOException {
//    IndexCommit commit = Utils.findLatest(directory);
//    List<String> segments = Utils.getSegments(directory, commit);
//    for (String segment : segments) {
//      BlurInputSplit split = new BlurInputSplit(directory.getHdfsDirPath(), segment, 0, Integer.MAX_VALUE);
//      splits.add(split);
//    }
//  }
}
