package com.nearinfinity.blur.manager;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
