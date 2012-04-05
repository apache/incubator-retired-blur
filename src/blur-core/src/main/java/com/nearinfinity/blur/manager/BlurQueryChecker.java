package com.nearinfinity.blur.manager;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import static com.nearinfinity.blur.utils.BlurConstants.*;

public class BlurQueryChecker {

  private static final Log LOG = LogFactory.getLog(BlurQueryChecker.class);

  private int _maxQueryRowFetch;
  private int _maxQueryRecordFetch;
  private int _maxQueryResultsFetch;

  public BlurQueryChecker(BlurConfiguration configuration) {
    _maxQueryResultsFetch = configuration.getInt(BLUR_QUERY_MAX_RESULTS_FETCH, 100);
    _maxQueryRowFetch = configuration.getInt(BLUR_QUERY_MAX_ROW_FETCH, 100);
    _maxQueryRecordFetch = configuration.getInt(BLUR_QUERY_MAX_RECORD_FETCH, 100);
  }

  public void checkQuery(BlurQuery blurQuery) throws BlurException {
    if (blurQuery.selector != null) {
      if (blurQuery.selector.recordOnly) {
        if (blurQuery.fetch > _maxQueryRecordFetch) {
          LOG.warn("Number of records requested to be fetched [{0}] is greater than the max allowed [{1}]", _maxQueryRecordFetch);
          blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
        }
      } else {
        if (blurQuery.fetch > _maxQueryRowFetch) {
          LOG.warn("Number of rows requested to be fetched [{0}] is greater than the max allowed [{1}]", _maxQueryRowFetch);
          blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
        }
      }
    }
    if (blurQuery.fetch > _maxQueryResultsFetch) {
      LOG.warn("Number of results requested to be fetched [{0}] is greater than the max allowed [{1}]", _maxQueryResultsFetch);
      blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
    }
    if (blurQuery.fetch > blurQuery.minimumNumberOfResults) {
      LOG.warn("Number of rows/records requested to be fetched [{0}] is greater than the minimum number of results [{1}]", blurQuery.fetch, blurQuery.minimumNumberOfResults);
      blurQuery.fetch = (int) blurQuery.minimumNumberOfResults;
    }
  }

}
