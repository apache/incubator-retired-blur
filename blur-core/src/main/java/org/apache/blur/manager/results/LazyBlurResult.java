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

package org.apache.blur.manager.results;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.FetchResult;
import org.apache.blur.thrift.generated.Selector;

/**
 * The {@link LazyBlurResult} adds a method to fetch the result with the client
 * that was used to execute the query.
 */
@SuppressWarnings("serial")
public class LazyBlurResult extends BlurResult {

  private final Client _client;

  public LazyBlurResult(BlurResult result, Client client) {
    super(result);
    _client = client;
  }

  public Client getClient() {
    return _client;
  }

  public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
    synchronized (_client) {
      return _client.fetchRow(table, selector);
    }
  }

}
