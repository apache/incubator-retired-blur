package com.nearinfinity.blur.lucene.index;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

public class TimeBasedIndexDeletionPolicy implements IndexDeletionPolicy {

  private static final Log LOG = LogFactory.getLog(TimeBasedIndexDeletionPolicy.class);

  private long maxAge;

  public TimeBasedIndexDeletionPolicy(long maxAge) {
    this.maxAge = maxAge;
  }

  @Override
  public void onInit(List<? extends IndexCommit> commits) throws IOException {
    onCommit(commits);
  }

  @Override
  public void onCommit(List<? extends IndexCommit> commits) throws IOException {
    IndexCommit current = commits.get(commits.size() - 1);
    int length;
    if (isTooOld(current.getTimestamp())) {
      // the current index is old enough that following generation can be
      // removed
      length = commits.size() - 1;
    } else {
      // the current index is NOT old enough, so therefore the next generation
      // (no matter the old can be removed)
      length = commits.size() - 2;
    }
    for (int i = 0; i < length; i++) {
      IndexCommit commit = commits.get(i);
      if (isTooOld(commit.getTimestamp())) {
        LOG.info("Removing old generation [" + commit.getGeneration() + "] for directory [" + commit.getDirectory() + "]");
        commit.delete();
      }
    }
  }

  private boolean isTooOld(long timestamp) {
    if (timestamp + maxAge < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

}
