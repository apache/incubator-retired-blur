package org.apache.blur.testsuite;

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

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.SimpleQuery;
import org.apache.thrift.TException;

public class SimpleQueryExample {

  public static void main(String[] args) throws BlurException, TException, IOException {
    String connectionStr = args[0];
    String tableName = args[1];
    String query = args[2];

    Iface client = BlurClient.getClient(connectionStr);

    final BlurQuery blurQuery = new BlurQuery();
    SimpleQuery simpleQuery = new SimpleQuery();
    blurQuery.setSimpleQuery(simpleQuery);
    simpleQuery.setQueryStr(query);
    BlurResults results = client.query(tableName, blurQuery);
    System.out.println("Total Results: " + results.totalResults);

    for (BlurResult result : results.getResults()) {
      System.out.println(result);
    }
  }
}
