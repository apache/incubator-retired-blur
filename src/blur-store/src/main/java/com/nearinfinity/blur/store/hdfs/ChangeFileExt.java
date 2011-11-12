package com.nearinfinity.blur.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ChangeFileExt {

  public static void main(String[] args) throws IOException {
    Path p = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(p.toUri(), new Configuration());
    FileStatus[] listStatus = fileSystem.listStatus(p);
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      fileSystem.rename(path, new Path(path.toString() + ".lf"));
    }
  }
  
}
