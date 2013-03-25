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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfoPerCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;

public class Utils {

  public static int getTermInfosIndexDivisor(Configuration conf) {
    return 128;
  }

  public static IndexCommit findLatest(Directory dir) throws IOException {
    Collection<IndexCommit> listCommits = DirectoryReader.listCommits(dir);
    if (listCommits.size() == 1) {
      return listCommits.iterator().next();
    }
    throw new RuntimeException("Multiple commit points not supported yet.");
  }

  public static List<String> getSegments(Directory dir, IndexCommit commit) throws CorruptIndexException, IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(dir, commit.getSegmentsFileName());
    List<String> result = new ArrayList<String>();
    for (SegmentInfoPerCommit info : infos) {
      result.add(info.info.name);
    }
    return result;
  }

//  public static IndexReader openSegmentReader(Directory directory, IndexCommit commit, String segmentName, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
//    SegmentInfos infos = new SegmentInfos();
//    infos.read(directory, commit.getSegmentsFileName());
//    SegmentInfo segmentInfo = null;
//    for (SegmentInfoPerCommit info : infos) {
//      if (segmentName.equals(info.info.name)) {
//        segmentInfo = info.info;
//        break;
//      }
//    }
//    if (segmentInfo == null) {
//      throw new RuntimeException("SegmentInfo for [" + segmentName + "] not found in directory [" + directory + "] for commit [" + commit + "]");
//    }
//    return SegmentReader.get(true, segmentInfo, termInfosIndexDivisor);
//  }
}
