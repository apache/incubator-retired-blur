package org.apache.blur.thrift.util;

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
import java.io.IOException;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.async.AsyncMethodCallback;
import org.apache.blur.thrift.AsyncClientPool;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.AsyncClient.query_call;
import org.apache.blur.thrift.generated.Blur.AsyncIface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Query;

public class SimpleAsyncQueryExample {

  public static void main(String[] args) throws BlurException, TException, IOException, InterruptedException {
    String connectionStr = args[0];
    String tableName = args[1];
    String queryStr = args[2];

    AsyncClientPool pool = new AsyncClientPool(10, 30000);

    AsyncIface asyncIface = pool.getClient(Blur.AsyncIface.class, connectionStr);

    final BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery(queryStr);
    blurQuery.setQuery(query);

    asyncIface.query(tableName, blurQuery, new AsyncMethodCallback<Blur.AsyncClient.query_call>() {
      @Override
      public void onError(Exception exception) {
        exception.printStackTrace();
      }

      @Override
      public void onComplete(query_call response) {
        try {
          BlurResults results = response.getResult();
          System.out.println("Total Results: " + results.totalResults);
          for (BlurResult result : results.getResults()) {
            System.out.println(result);
          }
        } catch (BlurException e) {
          e.printStackTrace();
        } catch (TException e) {
          e.printStackTrace();
        }
      }
    });
    Thread.sleep(10000);

  }
}
