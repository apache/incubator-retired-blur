package com.nearinfinity.blur.thrift;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.ExecutionContext;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public interface IfaceExtended extends Iface {

    void cancelQuery(ExecutionContext context, String table, long uuid) throws BlurException, TException;

    List<String> controllerServerList(ExecutionContext context) throws BlurException, TException;

    List<BlurQueryStatus> currentQueries(ExecutionContext context, String table) throws BlurException, TException;
    
    TableStats getTableStats(ExecutionContext context, String table) throws BlurException, TException;

    TableDescriptor describe(ExecutionContext context, String table) throws BlurException, TException;

    FetchResult fetchRow(ExecutionContext context, String table, Selector selector) throws BlurException, TException;

    void mutate(ExecutionContext context, RowMutation mutation) throws BlurException, TException;

    BlurResults query(ExecutionContext context, String table, BlurQuery blurQuery) throws BlurException, TException;

    long recordFrequency(ExecutionContext context, String table, String columnFamily, String columnName, String value)
            throws BlurException, TException;

    Schema schema(ExecutionContext context, String table) throws BlurException, TException;

    Map<String, String> shardServerLayout(ExecutionContext context, String table) throws BlurException, TException;

    List<String> shardServerList(ExecutionContext context) throws BlurException, TException;

    List<String> tableList(ExecutionContext context) throws BlurException, TException;

    List<String> terms(ExecutionContext context, String table, String columnFamily, String columnName,
            String startWith, short size) throws BlurException, TException;

}
