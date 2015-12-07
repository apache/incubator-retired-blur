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

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur.Client;
//import org.apache.blur.thrift.generated.CommandRequest;

public class ServerCommandExample {

  public static void main(String[] args) throws TException, IOException {
 /*   CommandRequest commandRequest = new CommandRequest();
    commandRequest.setName("cool");
    Client client = BlurClientManager.getClientPool().getClient(new Connection("localhost:40020"));
    client.executeCommand(commandRequest);
    
    TProtocol input = client.getInputProtocol();
    if (input.readBool()) {
      TProtocol output = client.getOutputProtocol();
      output.writeI64(1000);
      output.getTransport().flush();  
    }
    
    long t = 0;
    for (long l = 0; l < 1000; l++) {
      t+=input.readByte();
    }
    System.out.println(t);

    System.out.println(client.tableList());
 */ }

}
