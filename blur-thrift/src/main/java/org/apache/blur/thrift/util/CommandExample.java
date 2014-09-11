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
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Response;
import org.apache.blur.thrift.generated.TimeoutException;

public class CommandExample {

  public static void main(String[] args) throws BlurException, TException, IOException {
    Client client = BlurClientManager.getClientPool().getClient(new Connection("localhost:40010"));
    // String executionId = null;
    // while (true) {
    // try {
    // Response response;
    // if (executionId == null) {
    // response = client.execute("test", "wait", null);
    // } else {
    // System.out.println("Reconecting...");
    // response = client.reconnect(executionId);
    // }
    // System.out.println(response);
    // break;
    // } catch (TimeoutException ex) {
    // executionId = ex.getExecutionId();
    // }
    // }

    System.out.println(client.execute("docCount", null));
    // System.out.println(client.execute("test", "docCountNoCombine", null));
    // {
    // Response response = client.execute("test", "docCountAggregate", null);
    // long count = response.getValue().getValue().getLongValue();
    // System.out.println(count);
    // }
    // {
    // Response response = client.execute("test", "testBlurObject", null);
    // System.out.println(response);
    // }
  }
}
