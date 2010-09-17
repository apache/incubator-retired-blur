package com.nearinfinity.blur.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class DoNothingServer implements Iface {

    @Override
    public void appendRow(String table, Row row) throws BlurException, MissingShardException, TException {
        
    }

    @Override
    public void create(String table, TableDescriptor desc) throws BlurException, TException {
        
    }

    @Override
    public void createDynamicTermQuery(String table, String term, String query, boolean superQueryOn)
            throws BlurException, MissingShardException, TException {
        
    }

    @Override
    public void deleteDynamicTermQuery(String table, String term) throws BlurException, MissingShardException,
            TException {
        
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
    public Row fetchRow(String table, String id) throws BlurException, MissingShardException, TException {
        return null;
    }

    @Override
    public String getDynamicTermQuery(String table, String term) throws BlurException, MissingShardException,
            TException {
        return null;
    }

    @Override
    public List<String> getDynamicTerms(String table) throws BlurException, MissingShardException, TException {
        return null;
    }

    @Override
    public boolean isDynamicTermQuerySuperQuery(String table, String term) throws BlurException, MissingShardException,
            TException {
        return false;
    }

    @Override
    public void removeRow(String table, String id) throws BlurException, MissingShardException, TException {
        
    }

    @Override
    public void replaceRow(String table, Row row) throws BlurException, MissingShardException, TException {
        
    }

    @Override
    public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String postSuperFilter,
            String preSuperFilter, long start, int fetch, long minimumNumberOfHits, long maxQueryTime)
            throws BlurException, MissingShardException, TException {
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

}
