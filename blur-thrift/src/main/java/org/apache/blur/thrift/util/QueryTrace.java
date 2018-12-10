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
package org.apache.blur.thrift.util;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.blur.thrift.generated.BlurResults;
import org.apache.blur.thrift.generated.Query;
import org.apache.blur.trace.Trace;

public class QueryTrace {

  public static void main(String[] args) throws BlurException, TException {
    Iface client = BlurClient.getClient("localhost:40010");
    
    BlurQuery blurQuery = new BlurQuery();
    Query query = new Query();
    query.setQuery("*");
    blurQuery.setQuery(query);
    Trace.setupTrace(Long.toString(System.currentTimeMillis()));
    BlurResults blurResults = client.query("test_hdfs", blurQuery);
    System.out.println(blurResults);
    Trace.tearDownTrace();
  }

}
