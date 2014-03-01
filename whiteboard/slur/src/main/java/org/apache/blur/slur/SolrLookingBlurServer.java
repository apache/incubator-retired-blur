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
package org.apache.blur.slur;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.blur.thrift.generated.RowMutationType;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import com.google.common.collect.Lists;

public class SolrLookingBlurServer extends SolrServer {
  private static final long serialVersionUID = -7141309168500300896L;
  private static final int DEFAULT_OPTIMIZE_MAX_SEGMENTS = 3;
  private String connectionString;
  private String tableName;

  public SolrLookingBlurServer(String controllerConnectionStr, String table) {
    connectionString = controllerConnectionStr;
    tableName = table;
  }

  @Override
  public NamedList<Object> request(SolrRequest arg0) throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public void shutdown() {
    // Nothing to do
  }

  @Override
  public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException,
      IOException {
    return add(docs);
  }

  @Override
  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    UpdateResponse response = new UpdateResponse();

    try {
      long start = System.currentTimeMillis();
      if (docs.size() == 1) {
        client().mutate(RowMutationHelper.from(docs.iterator().next(), tableName));
      } else {
        client().mutateBatch(RowMutationHelper.from(docs, tableName));
      }
      response.setElapsedTime((System.currentTimeMillis() - start));
    } catch (Exception e) {
      throw new SolrServerException(e);
    }

    return response;
  }

  @Override
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    return add(doc);
  }

  @Override
  public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    return add(Lists.newArrayList(new SolrInputDocument[] { doc }));
  }

  @Override
  public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse addBeans(Collection<?> arg0, int arg1) throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse commit() throws SolrServerException, IOException {
    return new UpdateResponse();
  }

  @Override
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException,
      IOException {
    return new UpdateResponse();
  }

  @Override
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return new UpdateResponse();
  }

  @Override
  public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(ids);
  }

  @Override
  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    UpdateResponse response = new UpdateResponse();
    long start = System.currentTimeMillis();

    try {
      if (ids.size() == 1) {
        client().mutate(toDeleteMutation(ids.get(0)));
      } else {
        List<RowMutation> mutates = Lists.newArrayList();
        for (String id : ids) {
          mutates.add(toDeleteMutation(id));
        }
        client().mutateBatch(mutates);
      }
    } catch (Exception e) {
      throw new SolrServerException("Unable to delete docs by ids.", e);
    }
    response.setElapsedTime((System.currentTimeMillis() - start));
    return response;
  }

  private RowMutation toDeleteMutation(String id) {
    RowMutation mutate = new RowMutation();
    mutate.setRowId(id);
    mutate.setRowMutationType(RowMutationType.DELETE_ROW);
    mutate.setTable(tableName);
    return mutate;
  }

  @Override
  public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(id);
  }

  @Override
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return deleteById(Lists.newArrayList(id));
  }

  @Override
  public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public DocumentObjectBinder getBinder() {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse optimize() throws SolrServerException, IOException {
    return optimize(true, true, DEFAULT_OPTIMIZE_MAX_SEGMENTS);
  }

  @Override
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException,
      IOException {
    long start = System.currentTimeMillis();
    try {
      client().optimize(tableName, maxSegments);
    } catch (Exception e) {
      throw new SolrServerException(e);
    }
    UpdateResponse response = new UpdateResponse();
    response.setElapsedTime((System.currentTimeMillis() - start));
    return response;
  }

  @Override
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(waitFlush, waitSearcher, DEFAULT_OPTIMIZE_MAX_SEGMENTS);
  }

  @Override
  public SolrPingResponse ping() throws SolrServerException, IOException {
    SolrPingResponse response = new SolrPingResponse();
    long start = System.currentTimeMillis();
    try {
      client().ping();
    } catch (TException e) {
      throw new SolrServerException(e);
    }
    response.setElapsedTime((System.currentTimeMillis() - start));
    return response;
  }

  @Override
  public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public QueryResponse query(SolrParams params) throws SolrServerException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback)
      throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  @Override
  public UpdateResponse rollback() throws SolrServerException, IOException {
    throw new RuntimeException("Not Implemented.");
  }

  private Iface client() {
    return BlurClient.getClient(connectionString);
  }
}
