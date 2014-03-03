package org.apache.blur.slur;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Selector;
import org.apache.commons.collections.map.HashedMap;
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
    
    if(fieldString != null) {
      Map<String, Set<String>> famCols = Maps.newHashMap();
      String[] fields = fieldString.split(",");
      
      for(String field: fields) {
        String[] famCol = field.split("\\.");
        
        if(famCol.length != 2) {
          throw new IllegalArgumentException("Fields must be in a family.column format[" + field + "]");
        }
        if(!famCols.containsKey(famCol[0])) {
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
