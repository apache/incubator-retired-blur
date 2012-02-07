package com.nearinfinity.blur.mapreduce.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;

public class Utils {
  
//  public static void main(String[] args) throws IOException {
//    Directory dir = FSDirectory.open(new File("/tmp/small-multi-seg-index"));
//    IndexCommit commit = findLatest(dir);
//    List<String> segments = getSegments(dir,commit);
//    for (String segment : segments) {
//      IndexReader reader = openSegmentReader(dir, commit, segment, 128);
//      System.out.println(segment + "=" + reader.numDocs());
//      reader.close();
//    }
//  }
  
  public static int getTermInfosIndexDivisor(Configuration conf) {
    return 128;
  }
  
  public static IndexCommit findLatest(Directory dir) throws IOException {
    Collection<IndexCommit> listCommits = IndexReader.listCommits(dir);
    if (listCommits.size() == 1) {
      return listCommits.iterator().next();
    }
    throw new RuntimeException("Multiple commit points not supported yet.");
  }

  public static List<String> getSegments(Directory dir, IndexCommit commit) throws CorruptIndexException, IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(dir, commit.getSegmentsFileName());
    List<String> result = new ArrayList<String>();
    for (SegmentInfo info : infos) {
      result.add(info.name);
    }
    return result;
  }

  public static IndexReader openSegmentReader(Directory directory, IndexCommit commit, String segmentName, int termInfosIndexDivisor) throws CorruptIndexException, IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(directory, commit.getSegmentsFileName());
    SegmentInfo segmentInfo = null;
    for (SegmentInfo info : infos) {
      if (segmentName.equals(info.name)) {
        segmentInfo = info;
        break;
      }
    }
    if (segmentInfo == null) {
      throw new RuntimeException("SegmentInfo for [" + segmentName + "] not found in directory [" + directory + "] for commit [" + commit + "]");
    }
    return SegmentReader.get(true, segmentInfo, termInfosIndexDivisor);
  }
}
