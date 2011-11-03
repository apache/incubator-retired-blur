/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.thrift.BException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Record;
import com.nearinfinity.blur.thrift.generated.RecordMutation;
import com.nearinfinity.blur.thrift.generated.RecordMutationType;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.RowMutationType;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.SimpleQuery;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurUtil {

  private static final Log LOG = LogFactory.getLog(BlurUtil.class);
  private static final String UNKNOWN = "UNKNOWN";

  public static BlurQuery newSimpleQuery(String query) {
    BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    simpleQuery.setQueryStr(query);
    blurQuery.setSimpleQuery(simpleQuery);
    blurQuery.setSelector(new Selector());
    return blurQuery;
  }
  
  public static void createIfMissing(ZooKeeper zookeeper, String path) throws KeeperException, InterruptedException {
    if (zookeeper.exists(path, false) == null) {
      try {
        zookeeper.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          return;
        }
        throw e;
      }
    }
  }

  public static List<Long> getList(AtomicLongArray atomicLongArray) {
    if (atomicLongArray == null) {
      return null;
    }
    List<Long> counts = new ArrayList<Long>(atomicLongArray.length());
    for (int i = 0; i < atomicLongArray.length(); i++) {
      counts.add(atomicLongArray.get(i));
    }
    return counts;
  }

  public static void quietClose(Object... close) {
    if (close == null) {
      return;
    }
    for (Object object : close) {
      if (object != null) {
        close(object);
      }
    }
  }

  private static void close(Object object) {
    Class<? extends Object> clazz = object.getClass();
    Method method;
    try {
      method = clazz.getMethod("close", new Class[] {});
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      return;
    }
    try {
      method.invoke(object, new Object[] {});
    } catch (Exception e) {
      LOG.error("Error while trying to close object [{0}]", object, e);
    }
  }

  public static Selector newSelector(String locationId) {
    Selector selector = new Selector();
    selector.locationId = locationId;
    return selector;
  }

  public static RecordMutation newRecordMutation(String family, String recordId, Column... columns) {
    Record record = new Record();
    record.setRecordId(recordId);
    record.setFamily(family);
    for (Column column : columns) {
      record.addToColumns(column);
    }

    RecordMutation mutation = new RecordMutation();
    mutation.setRecordMutationType(RecordMutationType.REPLACE_ENTIRE_RECORD);
    mutation.setRecord(record);
    return mutation;
  }

  public static RowMutation newRowMutation(String table, String rowId, RecordMutation... mutations) {
    RowMutation mutation = new RowMutation();
    mutation.setRowId(rowId);
    mutation.setTable(table);
    mutation.setRowMutationType(RowMutationType.REPLACE_ROW);
    for (RecordMutation recordMutation : mutations) {
      mutation.addToRecordMutations(recordMutation);
    }
    return mutation;
  }

  public static Record newRecord(String family, String recordId, Column... columns) {
    Record record = new Record();
    record.setRecordId(recordId);
    record.setFamily(family);
    record.setColumns(Arrays.asList(columns));
    return record;
  }

  public static Row newRow(String rowId, Record... records) {
    Row row = new Row().setId(rowId);
    for (Record record : records) {
      row.addToRecords(record);
    }
    return row;
  }

  public static Column newColumn(String name, String value) {
    return new Column().setName(name).setValue(value);
  }

  public static byte[] toBytes(Serializable serializable) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      ObjectOutputStream stream = new ObjectOutputStream(outputStream);
      stream.writeObject(serializable);
      stream.close();
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Serializable fromBytes(byte[] bs) {
    ObjectInputStream stream = null;
    try {
      stream = new ObjectInputStream(new ByteArrayInputStream(bs));
      return (Serializable) stream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException e) {
          // eat
        }
      }
    }
  }

  public static List<Long> toList(AtomicLongArray atomicLongArray) {
    if (atomicLongArray == null) {
      return null;
    }
    int length = atomicLongArray.length();
    List<Long> result = new ArrayList<Long>(length);
    for (int i = 0; i < length; i++) {
      result.add(atomicLongArray.get(i));
    }
    return result;
  }

  public static AtomicLongArray getAtomicLongArraySameLengthAsList(List<?> list) {
    if (list == null) {
      return null;
    }
    return new AtomicLongArray(list.size());
  }

  public static BlurResults convertToHits(BlurResultIterable hitsIterable, BlurQuery query, AtomicLongArray facetCounts, ExecutorService executor, Selector selector,
      final Iface iface, final String table) throws InterruptedException, ExecutionException {
    BlurResults results = new BlurResults();
    results.setTotalResults(hitsIterable.getTotalResults());
    results.setShardInfo(hitsIterable.getShardInfo());
    if (query.minimumNumberOfResults > 0) {
      hitsIterable.skipTo(query.start);
      int count = 0;
      Iterator<BlurResult> iterator = hitsIterable.iterator();
      while (iterator.hasNext() && count < query.fetch) {
        results.addToResults(iterator.next());
        count++;
      }
    }
    if (results.results == null) {
      results.results = new ArrayList<BlurResult>();
    }
    if (facetCounts != null) {
      results.facetCounts = BlurUtil.toList(facetCounts);
    }
    if (selector != null) {
      List<Future<FetchResult>> futures = new ArrayList<Future<FetchResult>>();
      for (int i = 0; i < results.results.size(); i++) {
        BlurResult result = results.results.get(i);
        final Selector s = new Selector(selector);
        s.setLocationId(result.locationId);
        futures.add(executor.submit(new Callable<FetchResult>() {
          @Override
          public FetchResult call() throws Exception {
            return iface.fetchRow(table, s);
          }
        }));
      }
      for (int i = 0; i < results.results.size(); i++) {
        Future<FetchResult> future = futures.get(i);
        BlurResult result = results.results.get(i);
        result.setFetchResult(future.get());
      }
    }
    results.query = query;
    results.query.selector = selector;
    return results;
  }

  public static Query readQuery(byte[] bs) throws BException {
    return readObject(bs);
  }

  public static byte[] writeQuery(Query query) throws BException {
    return writeObject(query);
  }

  public static Sort readSort(byte[] bs) throws BException {
    return readObject(bs);
  }

  public static byte[] writeSort(Sort sort) throws BException {
    return writeObject(sort);
  }

  public static Filter readFilter(byte[] bs) throws BException {
    return readObject(bs);
  }

  public static byte[] writeFilter(Filter filter) throws BException {
    return writeObject(filter);
  }

  private static byte[] writeObject(Serializable o) throws BException {
    if (o == null) {
      return null;
    }
    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
      outputStream.writeObject(o);
      outputStream.close();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      throw new BException("Unknown error", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T readObject(byte[] bs) throws BException {
    if (bs == null) {
      return null;
    }
    ObjectInputStream inputStream = null;
    try {
      inputStream = new ObjectInputStream(new ByteArrayInputStream(bs));
      return (T) inputStream.readObject();
    } catch (IOException e) {
      throw new BException("Unknown error", e);
    } catch (ClassNotFoundException e) {
      throw new BException("Unknown error", e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          throw new BException("Unknown error", e);
        }
      }
    }
  }

  public static void setStartTime(BlurQuery query) {
    if (query.startTime == 0) {
      query.startTime = System.currentTimeMillis();
    }
  }

  public static String getVersion() {
    String path = "/META-INF/maven/com.nearinfinity.blur/blur-core/pom.properties";
    InputStream inputStream = BlurUtil.class.getResourceAsStream(path);
    if (inputStream == null) {
      return UNKNOWN;
    }
    Properties prop = new Properties();
    try {
      prop.load(inputStream);
    } catch (IOException e) {
      LOG.error("Unknown error while getting version.", e);
      return UNKNOWN;
    }
    Object verison = prop.get("version");
    if (verison == null) {
      return UNKNOWN;
    }
    return verison.toString();
  }
  
  public static void unlockForSafeMode(ZooKeeper zookeeper, String lockPath) throws InterruptedException, KeeperException {
    zookeeper.delete(lockPath, -1);
    LOG.info("Lock released.");
  }

  public static String lockForSafeMode(ZooKeeper zookeeper, String nodeName) throws KeeperException, InterruptedException {
    LOG.info("Getting safe mode lock.");
    final Object lock = new Object();
    String blurSafemodePath = ZookeeperPathConstants.getBlurSafemodePath();
    String newPath = zookeeper.create(blurSafemodePath + "/safemode-", nodeName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    while (true) {
      synchronized (lock) {
        List<String> children = new ArrayList<String>(zookeeper.getChildren(blurSafemodePath, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            synchronized (lock) {
              lock.notifyAll();
            }
          }
        }));
        Collections.sort(children);
        if (newPath.equals(blurSafemodePath + "/" + children.get(0))) {
          LOG.info("Lock aquired.");
          return newPath;
        } else {
          lock.wait();
        }
      }
    }
  }
}
