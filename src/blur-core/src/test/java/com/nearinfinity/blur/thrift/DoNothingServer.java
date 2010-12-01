package com.nearinfinity.blur.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Iface;

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
    public Hits search(String table, SearchQuery searchQuery) throws BlurException, MissingShardException, TException {
        return null;
    }

    @Override
    public void cancelSearch(long providedUuid) throws BlurException, TException {
        
    }

    @Override
    public void shutdownController(String node) throws BlurException, TException {
        
    }

    @Override
    public void shutdownShard(String node) throws BlurException, TException {
        
    }

    @Override
    public FetchResult fetchRow(String table, Selector selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        return null;
    }

    @Override
    public byte[] fetchRowBinary(String arg0, Selector selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        return null;
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
        return null;
    }

}
