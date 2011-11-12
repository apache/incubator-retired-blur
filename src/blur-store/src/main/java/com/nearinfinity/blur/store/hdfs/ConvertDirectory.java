package com.nearinfinity.blur.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

public class ConvertDirectory {

  public static void main(String[] args) throws CorruptIndexException, LockObtainFailedException, IOException {
    Path path = new Path(args[0]);
    convert(path);
  }

  public static void convert(Path path) throws IOException {
    FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
    if (!fileSystem.exists(path)) {
      System.out.println(path + " does not exists.");
      return;
    }
    FileStatus fileStatus = fileSystem.getFileStatus(path);
    if (fileStatus.isDir()) {
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus status : listStatus) {
        convert(status.getPath());
      }
    } else {
      System.out.println("Converting file [" + path + "]");
      HdfsMetaBlock block = new HdfsMetaBlock();
      block.realPosition = 0;
      block.logicalPosition = 0;
      block.length = fileStatus.getLen();
      FSDataOutputStream outputStream = fileSystem.append(path);
      block.write(outputStream);
      outputStream.writeInt(1);
      outputStream.writeLong(fileStatus.getLen());
      outputStream.writeInt(HdfsFileWriter.VERSION);
      outputStream.close();
    }
  }
}
