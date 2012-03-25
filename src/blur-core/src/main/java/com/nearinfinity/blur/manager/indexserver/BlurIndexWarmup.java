package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;

import com.nearinfinity.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public abstract class BlurIndexWarmup {

  /**
   * Once the reader has be warmed up, release() must be called on the ReleaseReader even if an exception occurs.
   * @param table the table name.
   * @param shard the shard name.
   * @param reader thread reader inself.
   * @param isClosed to check if the shard has been migrated to another node.
   * @param releaseReader to release the handle on the reader.
   * @throws IOException
   * 
   * @deprecated
   */
  public void warmBlurIndex(String table, String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    
  }

  
  /**
   * Once the reader has be warmed up, release() must be called on the ReleaseReader even if an exception occurs.
   * @param table the table descriptor.
   * @param shard the shard name.
   * @param reader thread reader inself.
   * @param isClosed to check if the shard has been migrated to another node.
   * @param releaseReader to release the handle on the reader.
   * @throws IOException
   */
  public void warmBlurIndex(TableDescriptor table, String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    warmBlurIndex(table.name, shard, reader, isClosed, releaseReader);
  }

}
