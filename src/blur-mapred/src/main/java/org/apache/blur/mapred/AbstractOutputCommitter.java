package org.apache.blur.mapred;

import java.io.IOException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.mapreduce.lib.BlurOutputFormat;
import org.apache.blur.utils.BlurConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptID;

public abstract class AbstractOutputCommitter extends OutputCommitter {

  private final static Log LOG = LogFactory.getLog(AbstractOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    // look through all the shards for attempts that need to be cleaned up.
    // also find all the attempts that are finished
    // then rename all the attempts jobs to commits
    LOG.info("Commiting Job [{0}]",jobContext.getJobID());
    Configuration configuration = jobContext.getConfiguration();
    Path tableOutput = BlurOutputFormat.getOutputPath(configuration);
    FileSystem fileSystem = tableOutput.getFileSystem(configuration);
    for (FileStatus fileStatus : fileSystem.listStatus(tableOutput)) {
      if (isShard(fileStatus)) {
        commitJob(jobContext, fileStatus.getPath());
      }
    }

  }

  private void commitJob(JobContext jobContext, Path shardPath) throws IOException {
    FileSystem fileSystem = shardPath.getFileSystem(jobContext.getConfiguration());
    FileStatus[] listStatus = fileSystem.listStatus(shardPath);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      String name = path.getName();
      if (fileStatus.isDir() && name.endsWith(".task_complete")) {
        String taskAttemptName = getTaskAttemptName(name);
        TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptName);
        if (taskAttemptID.getJobID().equals(jobContext.getJobID())) {
          fileSystem.rename(path, new Path(shardPath, taskAttemptName + ".commit"));
          LOG.info("Committing [{0}] in path [{1}]", taskAttemptID, path);
        }
      }
    }
  }

  private String getTaskAttemptName(String name) {
    int lastIndexOf = name.lastIndexOf('.');
    return name.substring(0, lastIndexOf);
  }

  private boolean isShard(FileStatus fileStatus) {
    return isShard(fileStatus.getPath());
  }

  private boolean isShard(Path path) {
    return path.getName().startsWith(BlurConstants.SHARD_PREFIX);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    System.out.println("abortJob");
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    System.out.println("cleanupJob");
  }

}
