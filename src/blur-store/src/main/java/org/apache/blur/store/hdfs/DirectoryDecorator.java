package org.apache.blur.store.hdfs;

import org.apache.lucene.store.Directory;

public interface DirectoryDecorator {
  
  Directory getOriginalDirectory();

}
