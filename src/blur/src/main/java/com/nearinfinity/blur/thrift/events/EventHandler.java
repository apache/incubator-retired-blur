package com.nearinfinity.blur.thrift.events;

import com.nearinfinity.blur.thrift.BlurAdminServer;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;

public interface EventHandler {
    
    boolean beforeAppendRow(BlurAdminServer server, String table, Row row);
    void afterAppendRow(BlurAdminServer server, String table, Row row);
    
    boolean beforeReplaceRow(BlurAdminServer server, String table, Row row);
    void afterReplaceRow(BlurAdminServer server, String table, Row row);
    
    boolean beforeRemoveRow(BlurAdminServer server, String table, String id);
    void afterRemoveRow(BlurAdminServer server, String table, String id);
    
    boolean beforeCancelSearch(BlurAdminServer server, long providedUuid);
    void afterCancelSearch(BlurAdminServer server, long providedUuid);
    
    boolean beforeFetchRow(BlurAdminServer server, String table, String id);
    FetchResult afterFetchRow(BlurAdminServer server, String table, String id, FetchResult fetchResult);
    
    boolean beforeSearch(BlurAdminServer server, String table, SearchQuery query);
    Hits afterSearch(BlurAdminServer server, String table, SearchQuery query, Hits hits);

}
