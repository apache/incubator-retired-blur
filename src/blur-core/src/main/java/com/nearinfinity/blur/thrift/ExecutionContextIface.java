package com.nearinfinity.blur.thrift;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;

import com.nearinfinity.blur.concurrent.ExecutionContext;
import com.nearinfinity.blur.concurrent.ExecutionContext.RecordTime;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
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

public abstract class ExecutionContextIface extends TableAdmin implements IfaceExtended {

    private static final Log METRICS = LogFactory.getLog("METRICS-FILE");
    private static final Log LOG = LogFactory.getLog(ExecutionContextIface.class);

    private BlockingQueue<ExecutionContext> _contexts = new LinkedBlockingQueue<ExecutionContext>();
    private AtomicBoolean _running = new AtomicBoolean();
    private Thread _daemon;
    private boolean _loggingDisabled = true;

    public void init() {
        _running.set(true);
        _daemon = new Thread(new Runnable() {
            @Override
            public void run() {
                while (_running.get()) {
                    try {
                        log(_contexts.take());
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        _daemon.setName("Execution Context Logger");
        _daemon.setDaemon(true);
        _daemon.start();
    }

    private void log(ExecutionContext context) {
        if (_loggingDisabled) {
            return;
        }
        long now = System.nanoTime();
        List<RecordTime> times = context.getTimes();
        int size = times.size();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < size; i++) {
            RecordTime recordTime = times.get(i);
            builder.setLength(0);
            builder.append(now).append(',');
            builder.append(recordTime._e.getClass().getName()).append(',');
            builder.append(recordTime._e.name()).append(',');
            builder.append(recordTime._call).append(',');
            builder.append(recordTime._now).append(',');
            builder.append(recordTime._timeNs);
            METRICS.info(builder.toString());
        }
    }

    private ExecutionContext getContext() {
        return new ExecutionContext();
    }

    private void record(ExecutionContext context) {
        try {
            _contexts.put(context);
        } catch (InterruptedException e) {
            LOG.error("InterruptedException",e);
            return;
        }
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
    public TableStats getTableStats(String table) throws BlurException, TException {
    	ExecutionContext context = getContext();
        try {
            return getTableStats(context, table);
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
    public void mutate(RowMutation mutation) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            mutate(context, mutation);
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
    public List<String> shardServerList(String cluster) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return shardServerList(context, cluster);
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

    @Override
    public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            mutateBatch(context, mutations);
        } finally {
            record(context);
        }
    }

    @Override
    public List<String> shardClusterList() throws BlurException, TException {
        ExecutionContext context = getContext();
        try {
            return shardClusterList(context);
        } finally {
            record(context);
        }
    }
    
    
}
