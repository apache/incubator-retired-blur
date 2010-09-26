package com.nearinfinity.blur.thrift.events;

import com.nearinfinity.blur.thrift.BlurAdminServer;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.Selector;

public class EmptyEventHandler implements EventHandler {

    @Override
    public void afterAppendRow(BlurAdminServer server, String table, Row row) {

    }

    @Override
    public void afterCancelSearch(BlurAdminServer server, long providedUuid) {

    }

    @Override
    public FetchResult afterFetchRow(BlurAdminServer server, String table, Selector selector, FetchResult fetchResult) {
        return fetchResult;
    }

    @Override
    public void afterRemoveRow(BlurAdminServer server, String table, String id) {

    }

    @Override
    public void afterReplaceRow(BlurAdminServer server, String table, Row row) {

    }

    @Override
    public Hits afterSearch(BlurAdminServer server, String table, SearchQuery query, Hits hits) {
        return hits;
    }

    @Override
    public boolean beforeAppendRow(BlurAdminServer server, String table, Row row) {
        return true;
    }

    @Override
    public boolean beforeCancelSearch(BlurAdminServer server, long providedUuid) {
        return true;
    }

    @Override
    public boolean beforeFetchRow(BlurAdminServer server, String table, Selector selector) {
        return true;
    }

    @Override
    public boolean beforeRemoveRow(BlurAdminServer server, String table, String id) {
        return true;
    }

    @Override
    public boolean beforeReplaceRow(BlurAdminServer server, String table, Row row) {
        return true;
    }

    @Override
    public boolean beforeSearch(BlurAdminServer server, String table, SearchQuery query) {
        return true;
    }

}
