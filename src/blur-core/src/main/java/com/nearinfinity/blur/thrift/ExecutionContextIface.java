package com.nearinfinity.blur.thrift;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.ExecutionContext;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurQuerySuggestions;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public abstract class ExecutionContextIface implements IfaceExtended {
    
//    private PrintWriter _recorder;

    public ExecutionContextIface() {
//        try {
//            _recorder = new PrintWriter(new File("metrics.txt"));
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        }
    }

    private ExecutionContext getContext() {
        return new ExecutionContext();
    }
    
    private void record(ExecutionContext context) {
//        long now = System.nanoTime();
//        List<RecordTime> times = context.getTimes();
//        int size = times.size();
//        for (int i = 0; i < size; i++) {
//            RecordTime recordTime = times.get(i);
//            _recorder.print(now);
//            _recorder.print('\t');
//            _recorder.print(recordTime._e.getClass().getName());
//            _recorder.print('.');
//            _recorder.print(recordTime._e.name());
//            _recorder.print('\t');
//            _recorder.print(recordTime._call);
//            _recorder.print('\t');
//            _recorder.print(recordTime._now);
//            _recorder.print('\t');
//            _recorder.println(recordTime._timeNs);
//        }
    }
    
    @Override
    public void cancelQuery(String table, long uuid) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            cancelQuery(context, table, uuid);
        } finally {
            record(context);
        }
    }

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return controllerServerList(context);
        } finally {
            record(context);
        }
    }

    @Override
    public List<BlurQueryStatus> currentQueries(String table) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return currentQueries(context, table);
        } finally {
            record(context);
        }
    }

    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return describe(context, table);
        } finally {
            record(context);
        }
    }

    @Override
    public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return fetchRow(context, table, selector);
        } finally {
            record(context);
        }
    }

    @Override
    public void mutate(String table, List<RowMutation> mutations) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            mutate(context, table, mutations);
        } finally {
            record(context);
        }
    }

    @Override
    public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return query(context, table, blurQuery);
        } finally {
            record(context);
        }
    }

    @Override
    public BlurQuerySuggestions querySuggestions(String table, BlurQuery blurQuery) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return querySuggestions(context, table, blurQuery);
        } finally {
            record(context);
        }
    }

    @Override
    public long recordFrequency(String table, String columnFamily, String columnName, String value)
            throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return recordFrequency(context, table, columnFamily, columnName, value);
        } finally {
            record(context);
        }
    }

    @Override
    public Schema schema(String table) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return schema(context, table);
        } finally {
            record(context);
        }
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return shardServerLayout(context, table);
        } finally {
            record(context);
        }
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return shardServerList(context);
        } finally {
            record(context);
        }
    }

    @Override
    public List<String> tableList() throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return tableList(context);
        } finally {
            record(context);
        }
    }

    @Override
    public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size)
            throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return terms(context, table, columnFamily, columnName, startWith, size);
        } finally {
            record(context);
        }
    }

}
