/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.jmeter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TBinaryProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryBuffer;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Column;
import org.apache.blur.thrift.generated.FetchRecordResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.FetchRowResult;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.blur.thrift.generated.Selector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class BlurSamplerClient extends AbstractJavaSamplerClient implements Serializable {

  private static final Log LOG = LogFactory.getLog(BlurSamplerClient.class);

  private static List<Entry<String, String>> _fieldValueCache = new ArrayList<>();
  private static final long serialVersionUID = 1L;
  private String _table;
  private Iface _client;
  private int _fieldValueCacheSize = 10000;
  private ThreadLocal<Random> _random = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  @Override
  public Arguments getDefaultParameters() {
    Arguments arguments = new Arguments();
    arguments.addArgument("ZooKeeperConnection", "localhost",
        "The ZooKeeper connection string to the Blur cluster you want to test.");
    arguments.addArgument("Table", "test");
    return arguments;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    String zkConnectionString = context.getParameter("ZooKeeperConnection");
    _table = context.getParameter("Table");
    _client = BlurClient.getClientFromZooKeeperConnectionStr(zkConnectionString);
  }

  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    SampleResult sampleResult = new SampleResult();
    BlurResults blurResults = null;
    try {
      BlurQuery blurQuery = getBlurQuery();
      sampleResult.sampleStart();
      blurResults = _client.query(_table, blurQuery);
      sampleResult.sampleEnd();
      int size = getBytes(blurResults);
      sampleResult.setBytes(size);
      sampleResult.setSuccessful(true);
      sampleResult.setResponseOK();
    } catch (Throwable t) {
      sampleResult.setResponseMessage("Exception " + t.getMessage());
      sampleResult.setSuccessful(false);
      LOG.error("Unknown error.", t);
    } finally {
      processResults(blurResults);
    }
    return sampleResult;
  }

  private int getBytes(BlurResults blurResults) {
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    TProtocol oprot = new TBinaryProtocol(trans);
    try {
      blurResults.write(oprot);
    } catch (TException t) {
      LOG.error("Unknown error.", t);
    }
    return trans.length();
  }

  private BlurQuery getBlurQuery() {
    BlurQuery blurQuery = new BlurQuery();
    blurQuery.setSelector(new Selector());
    Query query = new Query();
    query.setQuery(getQueryString());
    blurQuery.setQuery(query);
    return blurQuery;
  }

  private String getQueryString() {
    Random random = getRandom();
    int numberOfClauses = random.nextInt(9) + 1;
    StringBuilder builder = new StringBuilder();
    int size = _fieldValueCache.size();
    if (size == 0) {
      return "*";
    }
    for (int i = 0; i < numberOfClauses; i++) {
      int index = random.nextInt(size);
      Entry<String, String> entry = _fieldValueCache.get(index);
      builder.append("<" + entry.getKey() + ":" + entry.getValue() + "> ");
    }
    String query = builder.toString();
    // LOG.info("Query [" + query + "]");
    return query;
  }

  private void processResults(BlurResults blurResults) {
    if (blurResults == null) {
      return;
    }
    for (BlurResult blurResult : blurResults.getResults()) {
      processResult(blurResult);
    }
  }

  private void processResult(BlurResult blurResult) {
    if (blurResult == null) {
      return;
    }
    processResult(blurResult.getFetchResult());
  }

  private void processResult(FetchResult fetchResult) {
    if (fetchResult == null) {
      return;
    }
    processResult(fetchResult.getRowResult());
    processResult(fetchResult.getRecordResult());
  }

  private void processResult(FetchRecordResult recordResult) {
    if (recordResult == null) {
      return;
    }
    addRowId(recordResult.getRowid());
    processResult(recordResult.getRecord());
  }

  private void processResult(FetchRowResult rowResult) {
    if (rowResult == null) {
      return;
    }
    processResult(rowResult.getRow());
  }

  private void processResult(Row row) {
    if (row == null) {
      return;
    }
    addRowId(row.getId());
    processResult(row.getRecords());
  }

  private void processResult(List<Record> records) {
    if (records == null) {
      return;
    }
    for (Record record : records) {
      processResult(record);
    }
  }

  private void addRowId(String id) {
    addFieldToQuery("rowid", id);
  }

  private void processResult(Record record) {
    if (record == null) {
      return;
    }
    addRecordId(record.getRecordId());
    List<Column> columns = record.getColumns();
    if (columns != null) {
      for (Column column : columns) {
        addFieldToQuery(record.getFamily() + "." + column.getName(), column.getValue());
      }
    }
  }

  private void addRecordId(String id) {
    addFieldToQuery("recordid", id);
  }

  private void addFieldToQuery(String fieldName, String value) {
    if (StringUtils.isEmpty(fieldName) || StringUtils.isEmpty(value)) {
      return;
    }
    Entry<String, String> e = toEntry(fieldName, value);
    // LOG.info("Adding [" + fieldName + "] [" + value + "]");
    synchronized (_fieldValueCache) {
      if (_fieldValueCache.size() >= _fieldValueCacheSize) {
        Random random = getRandom();
        int index = random.nextInt(_fieldValueCache.size());
        _fieldValueCache.set(index, e);
      } else {
        _fieldValueCache.add(e);
      }
    }
  }

  private Random getRandom() {
    return _random.get();
  }

  private static Entry<String, String> toEntry(String fieldName, String value) {
    return new Entry<String, String>() {

      @Override
      public String setValue(String value) {
        throw new RuntimeException("Not implemented.");
      }

      @Override
      public String getValue() {
        return value;
      }

      @Override
      public String getKey() {
        return fieldName;
      }
    };
  }
}
