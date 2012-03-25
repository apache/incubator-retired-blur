package com.nearinfinity.blur.manager.indexserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.FieldOption;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.WarmUpByFieldBounds;
import org.apache.lucene.index.WarmUpByFieldBoundsStatus;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.DistributedIndexServer.ReleaseReader;
import com.nearinfinity.blur.thrift.generated.ColumnPreCache;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;

public class DefaultBlurIndexWarmup extends BlurIndexWarmup {

  private static final Log LOG = LogFactory.getLog(DefaultBlurIndexWarmup.class);

  @Override
  public void warmBlurIndex(final TableDescriptor table, final String shard, IndexReader reader, AtomicBoolean isClosed, ReleaseReader releaseReader) throws IOException {
    try {
      ColumnPreCache columnPreCache = table.columnPreCache;
      List<String> preCacheCols = null;
      if (columnPreCache != null) {
        preCacheCols = columnPreCache.preCacheCols;
      }
      if (preCacheCols == null) {
        LOG.info("No pre cache defined, precache all fields.");
        preCacheCols = new ArrayList<String>(reader.getFieldNames(FieldOption.INDEXED));
        preCacheCols.remove(BlurConstants.ROW_ID);
        preCacheCols.remove(BlurConstants.RECORD_ID);
        preCacheCols.remove(BlurConstants.PRIME_DOC);
        preCacheCols.remove(BlurConstants.SUPER);
      }
      
      WarmUpByFieldBounds warmUpByFieldBounds = new WarmUpByFieldBounds();
      WarmUpByFieldBoundsStatus status = new WarmUpByFieldBoundsStatus() {
        @Override
        public void complete(String name, Term start, Term end, long startPosition, long endPosition, long totalBytesRead, long nanoTime, AtomicBoolean isClosed) {
          double bytesPerNano = totalBytesRead / (double) nanoTime;
          double mBytesPerNano = bytesPerNano / 1024 / 1024;
          double mBytesPerSecond = mBytesPerNano * 1000000000.0;
          if (totalBytesRead > 0) {
            LOG.info("Precached field [{0}] in table [{1}] shard [{2}] file [{3}], [{4}] bytes cached at [{5} MB/s]", start.field(), table.name, shard, name, totalBytesRead,
                mBytesPerSecond);
          }
        }
      };
      if (preCacheCols != null) {
        for (String field : preCacheCols) {
          warmUpByFieldBounds.warmUpByField(isClosed, new Term(field), reader, status);
        }
      }
    } finally {
      releaseReader.release();
    }
  }

}
