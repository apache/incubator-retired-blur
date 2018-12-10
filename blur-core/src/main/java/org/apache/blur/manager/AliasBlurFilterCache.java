package org.apache.blur.manager;

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
import static org.apache.blur.utils.BlurConstants.BLUR_FILTER_ALIAS;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.filter.FilterCache;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * This implementation on {@link BlurFilterCache} does nothing and it is the
 * default {@link BlurFilterCache}.
 */
public class AliasBlurFilterCache extends BlurFilterCache {

  private static final Log LOG = LogFactory.getLog(AliasBlurFilterCache.class);

  static class FilterKey {
    final String _table;
    final String _filterStr;

    FilterKey(String _table, String _filterStr) {
      this._table = _table;
      this._filterStr = _filterStr;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_filterStr == null) ? 0 : _filterStr.hashCode());
      result = prime * result + ((_table == null) ? 0 : _table.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FilterKey other = (FilterKey) obj;
      if (_filterStr == null) {
        if (other._filterStr != null)
          return false;
      } else if (!_filterStr.equals(other._filterStr))
        return false;
      if (_table == null) {
        if (other._table != null)
          return false;
      } else if (!_table.equals(other._table))
        return false;
      return true;
    }

  }

  private final Map<FilterKey, Filter> _preFilterCacheMap;
  private final Map<FilterKey, Filter> _postFilterCacheMap;
  private final ConcurrentMap<String, ConcurrentMap<String, String>> _tableAliasFilterMap = new ConcurrentHashMap<String, ConcurrentMap<String, String>>();

  public AliasBlurFilterCache(BlurConfiguration configuration) {
    super(configuration);
    long _cacheEntries = 100;
    _preFilterCacheMap = new ConcurrentLinkedHashMap.Builder<FilterKey, Filter>()
        .maximumWeightedCapacity(_cacheEntries).build();
    _postFilterCacheMap = new ConcurrentLinkedHashMap.Builder<FilterKey, Filter>().maximumWeightedCapacity(
        _cacheEntries).build();
    Map<String, String> properties = configuration.getProperties();
    for (Entry<String, String> entry : properties.entrySet()) {
      if (isFilterAlias(entry.getKey())) {
        String value = entry.getValue();
        if (value == null || value.isEmpty()) {
          continue;
        }
        String name = getFilterAlias(entry.getKey());
        int index = name.indexOf('.');
        String table = name.substring(0, index);
        String alias = name.substring(index + 1);
        ConcurrentMap<String, String> aliasFilterMap = _tableAliasFilterMap.get(table);
        if (aliasFilterMap == null) {
          aliasFilterMap = new ConcurrentHashMap<String, String>();
          _tableAliasFilterMap.put(table, aliasFilterMap);
        }

        aliasFilterMap.put(alias, value);
      }
    }
  }

  @Override
  public Filter storePreFilter(String table, String filterStr, Filter filter, FilterParser filterParser)
      throws ParseException {
    if (filter instanceof QueryWrapperFilter) {
      QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filter;
      Query query = queryWrapperFilter.getQuery();
      Filter newFilter = buildNewFilter(query, _tableAliasFilterMap.get(table), filterParser);
      FilterKey key = new FilterKey(table, filterStr);
      _preFilterCacheMap.put(key, newFilter);
      return newFilter;
    }
    return filter;
  }

  @Override
  public Filter storePostFilter(String table, String filterStr, Filter filter, FilterParser filterParser)
      throws ParseException {
    return filter;
  }

  @Override
  public Filter fetchPreFilter(String table, String filterStr) {
    FilterKey filterKey = new FilterKey(table, filterStr);
    return _preFilterCacheMap.get(filterKey);
  }

  @Override
  public Filter fetchPostFilter(String table, String filterStr) {
    FilterKey filterKey = new FilterKey(table, filterStr);
    return _postFilterCacheMap.get(filterKey);
  }

  @Override
  public void closing(String table, String shard, BlurIndex index) {
    _tableAliasFilterMap.remove(table);
  }

  @Override
  public void opening(String table, String shard, BlurIndex index) {
    Map<String, String> properties = _configuration.getProperties();
    for (Entry<String, String> entry : properties.entrySet()) {
      if (isFilterAlias(entry.getKey())) {
        String value = entry.getValue();
        if (value == null || value.isEmpty()) {
          continue;
        }
        String filterAlias = getFilterAlias(entry.getKey());
        String filterQuery = value;

        Map<String, String> map = getThisTablesMap(table);
        LOG.info("Loading filter alias [{0}] with query [{1}] for table [{2}]", filterAlias, filterQuery, table);
        map.put(filterAlias, filterQuery);
      }
    }
  }

  private Map<String, String> getThisTablesMap(String table) {
    _tableAliasFilterMap.putIfAbsent(table, new ConcurrentHashMap<String, String>());
    return _tableAliasFilterMap.get(table);
  }

  private String getFilterAlias(String key) {
    return key.substring(BLUR_FILTER_ALIAS.length());
  }

  private boolean isFilterAlias(String key) {
    return key.startsWith(BLUR_FILTER_ALIAS);
  }

  private Filter buildNewFilter(Query query, ConcurrentMap<String, String> filterAlias, FilterParser filterParser)
      throws ParseException {
    if (query instanceof BooleanQuery) {
      BooleanQuery booleanQuery = (BooleanQuery) query;
      BooleanFilter booleanFilter = new BooleanFilter();
      for (BooleanClause clause : booleanQuery.clauses()) {
        booleanFilter.add(buildNewFilter(clause.getQuery(), filterAlias, filterParser), clause.getOccur());
      }
      return booleanFilter;
    } else if (query instanceof TermQuery) {
      TermQuery termQuery = (TermQuery) query;
      Term term = termQuery.getTerm();
      String key = term.toString();
      String queryStr = filterAlias.get(key);
      if (queryStr == null) {
        return new QueryWrapperFilter(termQuery);
      }
      String id = getId(key);
      return new FilterCache(id, new QueryWrapperFilter(filterParser.parse(queryStr)));
    } else {
      return new QueryWrapperFilter(query);
    }
  }

  private String getId(String key) {
    return key.replace(':', '-');
  }

}