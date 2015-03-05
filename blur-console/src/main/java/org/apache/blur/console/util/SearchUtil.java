package org.apache.blur.console.util;

import org.apache.blur.console.model.ResultRow;
import org.apache.blur.console.model.User;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.*;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.user.UserContext;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.*;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class SearchUtil {
  private static final String TOTAL_KEY = "total";
  private static final String DATA_KEY = "results";
  private static final String FAMILY_KEY = "families";
  private static final String TIME_KEY = "time";

  private static final String ROW_ROW_OPTION = "rowrow";
  private static final String RECORD_RECORD_OPTION = "recordrecord";

  public static Map<String, Object> search(Map<String, String[]> params, User user) throws IOException, TException {
    String table = HttpUtil.getFirstParam(params.get("table"));
    String query = HttpUtil.getFirstParam(params.get("query"));
    String rowQuery = HttpUtil.getFirstParam(params.get("rowRecordOption"));
    String start = HttpUtil.getFirstParam(params.get("start"));
    String fetch = HttpUtil.getFirstParam(params.get("fetch"));
    String[] families = params.get("families[]");
    String securityUser = HttpUtil.getFirstParam(params.get("securityUser"));

    if (query.indexOf("rowid:") >= 0) {
      return fetchRow(table, query, families, user, securityUser);
    }

    return searchAndFetch(table, query, rowQuery, start, fetch, families, user, securityUser);
  }

  public static Map<String, Long> facetSearch(Map<String, String[]> params, User user) throws IOException, TException {
    String table = HttpUtil.getFirstParam(params.get("table"));
    String query = HttpUtil.getFirstParam(params.get("query"));
    String family = HttpUtil.getFirstParam(params.get("family"));
    String column = HttpUtil.getFirstParam(params.get("column"));
    String[] terms = params.get("terms[]");
    String rowQuery = HttpUtil.getFirstParam(params.get("rowRecordOption"));
    String securityUser = HttpUtil.getFirstParam(params.get("securityUser"));
    
    System.out.println(params);

    Iface client = Config.getClient(user, securityUser);

    BlurQuery blurQuery = new BlurQuery();

    Query q = new Query(query, ROW_ROW_OPTION.equalsIgnoreCase(rowQuery), ScoreType.SUPER, null, null);
    blurQuery.setQuery(q);
    blurQuery.setUserContext(user.getName());
    for(String term : terms) {
    	blurQuery.addToFacets(new Facet("+(+" + family + "." + column + ":(" + term + "))", 1000));
    }

    BlurResults blurResults = client.query(table, blurQuery);

    List<Long> facetCounts = blurResults.getFacetCounts();
    
    Map<String, Long> countMap = new HashMap<String, Long>();
    for (int i = 0; i < terms.length; i++) {
    	countMap.put(terms[i], facetCounts.get(i));
    }

    return countMap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, Object> searchAndFetch(String table, String query, String rowQuery, String start, String fetch, String[] families, User user, String securityUser) throws IOException, TException {
    try {
      Iface client = Config.getClient(user, securityUser);

      boolean recordsOnly = RECORD_RECORD_OPTION.equalsIgnoreCase(rowQuery);

      BlurQuery blurQuery = new BlurQuery();

      Query q = new Query(query, ROW_ROW_OPTION.equalsIgnoreCase(rowQuery), ScoreType.SUPER, null, null);
      blurQuery.setQuery(q);
      blurQuery.setStart(Long.parseLong(start));
      blurQuery.setFetch(Integer.parseInt(fetch));
      blurQuery.setUserContext(user.getName());

      Selector s = new Selector();
      s.setRecordOnly(recordsOnly);
      s.setColumnFamiliesToFetch(new HashSet<String>(Arrays.asList(families)));
      blurQuery.setSelector(s);

      Map<String, Object> results = new HashMap<String, Object>();
      long startTime = System.currentTimeMillis();
      BlurResults blurResults = client.query(table, blurQuery);
      results.put(TIME_KEY, System.currentTimeMillis() - startTime);

      results.put(TOTAL_KEY, blurResults.getTotalResults());

      Map<String, List> rows = new HashMap<String, List>();
      for (BlurResult result : blurResults.getResults()) {
        FetchResult fetchResult = result.getFetchResult();

        if (recordsOnly) {
          // Record Result
          FetchRecordResult recordResult = fetchResult.getRecordResult();
          Record record = recordResult.getRecord();

          String family = record.getFamily();

          List<Map<String, String>> fam = (List<Map<String, String>>) getFam(family, rows, recordsOnly);
          fam.add(buildRow(record.getColumns(), record.getRecordId()));
        } else {
          // Row Result
          FetchRowResult rowResult = fetchResult.getRowResult();
          Row row = rowResult.getRow();
          if (row.getRecords() == null || row.getRecords().size() == 0) {
            for (String family : families) {
              List<ResultRow> fam = (List<ResultRow>) getFam(family, rows, recordsOnly);
              getRow(row.getId(), fam);
            }
          } else {
            for (Record record : row.getRecords()) {
              String family = record.getFamily();

              List<ResultRow> fam = (List<ResultRow>) getFam(family, rows, recordsOnly);
              ResultRow rowData = getRow(row.getId(), fam);
              rowData.getRecords().add(buildRow(record.getColumns(), record.getRecordId()));
            }
          }
        }
      }

      results.put(FAMILY_KEY, new HashSet<String>(Arrays.asList(families)));
      results.put(DATA_KEY, rows);

      return results;
    } finally {
      UserContext.reset();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Map<String, Object> fetchRow(String table, String query, String[] families, User user, String securityUser) throws IOException, TException {
    try {
      Iface client = Config.getClient(user, securityUser);

      Selector selector = new Selector();
      String rowid = StringUtils.remove(query, "rowid:");
      selector.setRowId(rowid);
      selector.setColumnFamiliesToFetch(new HashSet<String>(Arrays.asList(families)));

      Map<String, Object> results = new HashMap<String, Object>();
      long startTime = System.currentTimeMillis();
      FetchResult fetchRow = client.fetchRow(table, selector);
      results.put(TIME_KEY, System.currentTimeMillis() - startTime);
      results.put(TOTAL_KEY, fetchRow.getRowResult().getRow() == null ? 0 : 1);

      Map<String, List> rows = new HashMap<String, List>();
      Row row = fetchRow.getRowResult().getRow();
      if (row != null && row.getRecords() != null) {
        for (Record record : row.getRecords()) {
          String family = record.getFamily();

          List<ResultRow> fam = (List<ResultRow>) getFam(family, rows, false);
          ResultRow rowData = getRow(row.getId(), fam);
          rowData.getRecords().add(buildRow(record.getColumns(), record.getRecordId()));
        }
      }
      results.put(DATA_KEY, rows);
      results.put(FAMILY_KEY, new HashSet<String>(Arrays.asList(families)));

      return results;
    } finally {
      UserContext.reset();
    }
  }

  private static Map<String, String> buildRow(List<Column> columns, String recordid) {
    Map<String, String> map = new TreeMap<String, String>();
    map.put("recordid", recordid);

    for (Column column : columns) {
      map.put(column.getName(), column.getValue());
    }

    return map;
  }

  @SuppressWarnings("rawtypes")
  private static List getFam(String fam, Map<String, List> results, boolean recordOnly) {
    List famResults = results.get(fam);

    if (famResults == null) {
      if (recordOnly) {
        famResults = new ArrayList<Map<String, String>>();
      } else {
        famResults = new ArrayList<ResultRow>();
      }
      results.put(fam, famResults);
    }

    return famResults;
  }

  private static ResultRow getRow(String rowid, List<ResultRow> rows) {
    ResultRow row = null;
    for (ResultRow r : rows) {
      if (r.getRowid().equals(rowid)) {
        row = r;
        break;
      }
    }

    if (row == null) {
      row = new ResultRow(rowid);
      rows.add(row);
    }

    return row;
  }
}
