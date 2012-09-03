package com.nearinfinity.blur.thrift;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

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
  public List<String> shardServerList(String cluster) throws BlurException, TException {
    return null;
  }

  @Override
  public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
    return null;
  }

  @Override
  public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
    return null;
  }

  @Override
  public void cancelQuery(String table, long providedUuid) throws BlurException, TException {

  }

  @Override
  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    return null;
  }

  @Override
  public List<BlurQueryStatus> currentQueries(String arg0) throws BlurException, TException {
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
  public List<String> terms(String arg0, String arg1, String arg2, String arg3, short arg4) throws BlurException, TException {
    return null;
  }

  @Override
  public void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {

  }

  @Override
  public void disableTable(String table) throws BlurException, TException {

  }

  @Override
  public void enableTable(String table) throws BlurException, TException {

  }

  @Override
  public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {

  }

  @Override
  public TableStats getTableStats(String table) throws BlurException, TException {
    return null;
  }

  @Override
  public void mutate(RowMutation mutation) throws BlurException, TException {

  }

  @Override
  public void mutateBatch(List<RowMutation> mutations) throws BlurException, TException {

  }

  @Override
  public List<String> shardClusterList() throws BlurException, TException {
    return null;
  }

  @Override
  public List<String> tableListByCluster(String cluster) throws BlurException, TException {
    return null;
  }

  @Override
  public BlurQueryStatus queryStatusById(String table, long uuid) throws BlurException, TException {
    return null;
  }

  @Override
  public List<Long> queryStatusIdList(String table) throws BlurException, TException {
    return null;
  }

  @Override
  public void optimize(String table, int numberOfSegmentsPerShard) throws BlurException, TException {

  }

  @Override
  public boolean isInSafeMode(String cluster) throws BlurException, TException {
    return false;
  }

  @Override
  public TableStats tableStats(String table) throws BlurException, TException {
    return null;
  }

  @Override
  public Map<String, String> configuration() throws BlurException, TException {
    return null;
  }
}
