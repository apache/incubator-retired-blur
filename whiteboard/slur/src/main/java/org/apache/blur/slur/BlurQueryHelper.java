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
package org.apache.blur.slur;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

import com.google.common.collect.Maps;

public class BlurQueryHelper {

  public static BlurQuery from(SolrParams p) {
    BlurQuery blurQuery = new BlurQuery();

    Query query = new Query();
    query.setRowQuery(false);

    maybeAddSelector(blurQuery, p);

    query.setQuery(p.get(CommonParams.Q));

    blurQuery.setQuery(query);
    return blurQuery;
  }

  private static void maybeAddSelector(BlurQuery blurQuery, SolrParams p) {
    String fieldString = p.get(CommonParams.FL);
    Selector selector = new Selector();
    selector.setRecordOnly(true);

    if (fieldString != null) {
      Map<String, Set<String>> famCols = Maps.newHashMap();
      String[] fields = fieldString.split(",");

      for (String field : fields) {
        String[] famCol = field.split("\\.");

        if (famCol.length != 2) {
          throw new IllegalArgumentException("Fields must be in a family.column format[" + field + "]");
        }
        if (!famCols.containsKey(famCol[0])) {
          famCols.put(famCol[0], new HashSet<String>());
        }
        Set<String> cols = famCols.get(famCol[0]);
        cols.add(famCol[1]);
      }
      selector.setColumnsToFetch(famCols);

    }
    blurQuery.setSelector(selector);
  }

}
