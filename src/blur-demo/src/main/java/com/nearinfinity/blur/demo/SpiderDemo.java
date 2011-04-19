package com.nearinfinity.blur.demo;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.BlurClientManager;
import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class SpiderDemo {
    
    private static final String CONNECTION_STR = "localhost:40020";
    private static final int MAX_QUEUE = 1024 * 16;
    private static String table = "employee_super_mart";
    private static BlockingQueue<BlurQuery> queries = new LinkedBlockingQueue<BlurQuery>();
    private static AtomicLong rowCounter = new AtomicLong();
    private static AtomicLong queryCounter = new AtomicLong();
    private static AtomicLong recordCounter = new AtomicLong();
    private static AtomicInteger queueSize = new AtomicInteger();
    private static int threads = 2;
    private static ExecutorService fetchPool = Executors.newFixedThreadPool(threads);
    private static ExecutorService pool = Executors.newFixedThreadPool(threads);

    public static void main(String[] args) throws Exception {
        while (true) {
            gotime();
        }
    }
    
    private static void gotime() throws InterruptedException {
        queries.clear();
        queueSize.set(0);
        BlurQuery seed = new BlurQuery();
        seed.queryStr = "title.toDate:99999999";
        queries.put(seed);
        rowCounter.set(0);
        queryCounter.set(0);
        recordCounter.set(0);
        runQueries(threads,pool);
        long rowCounts = 0;
        long queryCount = 0;
        long recordCounts = 0;
        long s = System.nanoTime();
        for (int i = 0; i < 3; i++) {
            Thread.sleep(3000);
            long now = System.nanoTime();
            double seconds = (now - s) / 1000000000.0;
            StringBuilder builder = new StringBuilder();
            builder.append("Rows [");
            rowCounts = getRate(seconds,rowCounts,rowCounter,builder);
            builder.append("/s]        Records [");
            recordCounts = getRate(seconds,recordCounts,recordCounter,builder);
            builder.append("/s]        Queries [");
            queryCount = getRate(seconds,queryCount,queryCounter,builder);
            builder.append("/s]");
            System.out.println(builder);
            s = now;
        }    
    }

    private static long getRate(double seconds, long orgCounts, AtomicLong counter, StringBuilder builder) {
        long count = counter.longValue();
        long value = count - orgCounts;
        double rate = (value / seconds);
        builder.append(format(rate));
        return count;
    }

    private static String format(double rate) {
        long l = (long) (rate * 10);
        double d = (double) l / 10;
        return buffer(Double.toString(d), 10);
    }

    private static String buffer(String s, int length) {
        while (s.length() < length) {
            s = " " + s;
        }
        return s;
    }

    private static void runQueries(int threads, ExecutorService pool) {
        for (int i = 0; i < threads; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            BlurClientManager.execute(CONNECTION_STR, new SpiderCommand());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }
    }

    public static class SpiderCommand extends BlurCommand<Void> {
        @Override
        public Void call(Client client) throws Exception {
            sample(client, client.query(table, getBlurQuery()));
            queryCounter.incrementAndGet();
            return null;
        }
    }

    public static BlurQuery getBlurQuery() throws InterruptedException {
        try {
            return queries.take();
        } finally {
            queueSize.decrementAndGet();
        }
    }

    public static void sample(Client client, BlurResults results) throws BlurException, TException, InterruptedException {
        for (final BlurResult result : results.results) {
            fetchPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        BlurClientManager.execute(CONNECTION_STR, new BlurCommand<Void>() {
                            @Override
                            public Void call(Client client) throws Exception {
                                sample(getRow(client,result));
                                return null;
                            }
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            
        }
    }

    private static void sample(Row row) throws InterruptedException {
        for (ColumnFamily columnFamily : row.columnFamilies) {
            sample(columnFamily);
        }
    }

    private static void sample(ColumnFamily columnFamily) throws InterruptedException {
        Map<String, Set<Column>> records = columnFamily.records;
        for (String recordid : records.keySet()) {
            sample(columnFamily.family,records.get(recordid));
        }
    }

    private static void sample(String family, Set<Column> record) throws InterruptedException {
        for (Column column : record) {
            recordCounter.incrementAndGet();
            sample(family,column);
        }
    }

    private static void sample(String family, Column column) throws InterruptedException {
        for (String value : column.values) {
            if (!isFull(queries)) {
                queries.put(new BlurQuery().setQueryStr(family + "." + column.name + ":(" + value + ")"));
                queueSize.incrementAndGet();
            }
        }
    }

    private static boolean isFull(BlockingQueue<BlurQuery> queue) {
        if (queueSize.intValue() >= MAX_QUEUE) {
            return true;
        }
        return false;
    }

    private static Row getRow(Client client, BlurResult result) throws BlurException, TException {
        FetchResult fetchRow = client.fetchRow(table, getSelector(result));
        rowCounter.incrementAndGet();
        return fetchRow.rowResult.row;
    }

    private static Selector getSelector(BlurResult result) {
        return new Selector().setLocationId(result.locationId);
    }

}
