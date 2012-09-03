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
import static com.nearinfinity.blur.lucene.LuceneConstant.LUCENE_VERSION;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;

import com.nearinfinity.blur.lucene.search.SuperParser;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.ScoreType;

public class QueryParserUtil {

  public static Query parseQuery(String query, boolean superQueryOn, Analyzer analyzer, Filter postFilter, Filter preFilter, ScoreType scoreType) throws ParseException {
    Query result = new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, preFilter, scoreType).parse(query);
    if (postFilter == null) {
      return result;
    }
    return new FilteredQuery(result, postFilter);
  }

  public static Filter parseFilter(String table, String filterStr, boolean superQueryOn, Analyzer analyzer, BlurFilterCache filterCache) throws ParseException, BlurException {
    if (filterStr == null) {
      return null;
    }
    synchronized (filterCache) {
      Filter filter;
      if (superQueryOn) {
        filter = filterCache.fetchPostFilter(table, filterStr);
      } else {
        filter = filterCache.fetchPreFilter(table, filterStr);
      }
      if (filter != null) {
        return filter;
      }
      filter = new QueryWrapperFilter(new SuperParser(LUCENE_VERSION, analyzer, superQueryOn, null, ScoreType.CONSTANT).parse(filterStr));
      if (superQueryOn) {
        filter = filterCache.storePostFilter(table, filterStr, filter);
      } else {
        filter = filterCache.storePreFilter(table, filterStr, filter);
      }
      return filter;
    }
  }
}
