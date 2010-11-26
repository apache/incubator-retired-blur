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
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Iface;

public class DoNothingServer implements Iface {

    @Override
    public void create(String table, TableDescriptor desc) throws BlurException, TException {
        
    }

    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        return null;
    }

    @Override
    public void disable(String table) throws BlurException, TException {
        
    }

    @Override
    public void drop(String table) throws BlurException, TException {
        
    }

    @Override
    public void enable(String table) throws BlurException, TException {
        
    }

    @Override
    public void removeRow(String table, String id) throws BlurException, MissingShardException, TException {
        
    }

    @Override
    public void replaceRow(String table, Row row) throws BlurException, MissingShardException, TException {
        
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
    public byte[] fetchRowBinary(String arg0, String arg1, byte[] arg2) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        return null;
    }

    @Override
    public void replaceRowBinary(String arg0, String arg1, byte[] arg2) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        
    }

    @Override
    public void batchUpdate(String arg0, String arg1, Map<String, String> arg2) throws BlurException,
            MissingShardException, TException {
        
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
        return null;
    }

}
