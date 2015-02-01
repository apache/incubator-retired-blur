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

import java.io.IOException;
import java.util.List;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TCompactProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TSocket;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransport;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.sasl.SaslHelper;

public class SaslClient {

  public static void main(String[] args) throws BlurException, TException, IOException {
    final String username = "user1";
    final String password = "passWord1";
    {
      // Thrift setup by hand
      TTransport transport = new TSocket("localhost", 40020);
      BlurConfiguration configuration = new BlurConfiguration();
      SaslHelper.setPlainUsername(configuration, username);
      SaslHelper.setPlainPassword(configuration, password);
      TTransport tSaslClientTransport = SaslHelper.getTSaslClientTransport(configuration, transport);
      TProtocol tprotocol = new TCompactProtocol(tSaslClientTransport);
      Client client = new Blur.Client(tprotocol);
      tSaslClientTransport.open();
      List<String> tableList = client.tableList();
      System.out.println("response = " + tableList);
      transport.close();
    }
    {
      {
        // Using Blur client without config set in the classpath.
        // No need to setup config in code if setup in classpath.
        BlurConfiguration configuration = new BlurConfiguration();
        SaslHelper.setPlainUsername(configuration, username);
        SaslHelper.setPlainPassword(configuration, password);
        BlurClient.init(configuration);
      }

      Iface client = BlurClient.getClient("localhost:40020");
      List<String> tableList = client.tableList();
      System.out.println("response = " + tableList);
    }

  }
}
