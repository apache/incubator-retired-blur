package org.apache.blur.mapred;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskAttemptContext;

public class BlurOutputCommitter extends AbstractOutputCommitter {

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {

  }

}
