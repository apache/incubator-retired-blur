package com.nearinfinity.blur.manager;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;

public class BlurQueryChecker {

  private int _maxQueryFetch;

  public BlurQueryChecker(BlurConfiguration configuration) {
    _maxQueryFetch = configuration.getInt("blur.query.max.fetch", 100);
  }

  public void checkQuery(BlurQuery blurQuery) throws BlurException {
    if (blurQuery.fetch > _maxQueryFetch) {
      throw new BlurException("Fetch amount too large [" + blurQuery.fetch + "] \"blur.query.max.fetch=" + _maxQueryFetch + "\"", null);
    }
  }

}
