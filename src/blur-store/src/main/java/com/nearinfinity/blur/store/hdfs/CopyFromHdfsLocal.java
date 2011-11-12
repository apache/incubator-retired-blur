package com.nearinfinity.blur.store.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.nearinfinity.blur.store.compressed.CompressedFieldDataDirectory;

public class CopyFromHdfsLocal {

  public static void main(String[] args) throws IOException {
    Path path = new Path(args[0]);
    HdfsDirectory src = new HdfsDirectory(path);
    
    for (String name : src.listAll()) {
      System.out.println(name);
    }
    
    CompressedFieldDataDirectory compressedDirectory = new CompressedFieldDataDirectory(src, new DefaultCodec(), 32768);
    Directory dest = FSDirectory.open(new File(args[1]));
    
    for (String name : compressedDirectory.listAll()) {
      compressedDirectory.copy(dest, name, name);
    }

  }

}
