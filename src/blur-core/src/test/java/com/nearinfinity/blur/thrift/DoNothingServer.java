package com.nearinfinity.blur.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FacetQuery;
import com.nearinfinity.blur.thrift.generated.FacetResult;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;

public class DoNothingServer implements Iface {

    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> tableList() throws BlurException, TException {
        return Arrays.asList("donothing");
    }

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return null;
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        return null;
    }

    @Override
    public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
        return null;
    }

    @Override
    public void cancelSearch(long providedUuid) throws BlurException, TException {
        
    }

    @Override
    public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
        return null;
    }

    @Override
    public byte[] fetchRowBinary(String arg0, Selector selector) throws BlurException, TException {
        return null;
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
        return null;
    }

    @Override
    public long recordFrequency(String arg0, String arg1, String arg2, String arg3) throws BlurException, TException {
        return 0;
    }

    @Override
    public Schema schema(String arg0) throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> terms(String arg0, String arg1, String arg2, String arg3, short arg4) throws BlurException,
            TException {
        return null;
    }

    @Override
    public FacetResult facetSearch(String arg0, FacetQuery arg1) throws BlurException, TException {
        return null;
    }

}
