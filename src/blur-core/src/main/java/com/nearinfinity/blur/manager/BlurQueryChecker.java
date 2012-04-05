package com.nearinfinity.blur.manager;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;

public class BlurQueryChecker {

  private static final Log LOG = LogFactory.getLog(BlurQueryChecker.class);

  private int _maxQueryFetch;

  public BlurQueryChecker(BlurConfiguration configuration) {
    _maxQueryFetch = configuration.getInt("blur.query.max.fetch", 100);
  }

  public void checkQuery(BlurQuery blurQuery) throws BlurException {
    if (blurQuery.fetch > _maxQueryFetch) {
      LOG.warn("Number of rows/records requested to be fetched [{0}] is greater than the max allowed [{1}]", _maxQueryFetch);
      blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
    }
    if (blurQuery.fetch > blurQuery.minimumNumberOfResults) {
      LOG.warn("Number of rows/records requested to be fetched [{0}] is greater than the minimum number of results [{1}]", blurQuery.fetch, blurQuery.minimumNumberOfResults);
      blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
    }
  }

}
