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
package org.apache.blur.server.platform;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.blur.thrift.BException;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandRequest;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandResponse;
import org.apache.blur.thrift.generated.BlurCommandRequest;
import org.apache.blur.thrift.generated.BlurCommandResponse;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;

public class CommandControllerServer implements Closeable {

  @Override
  public void close() throws IOException {
    
  }

  public BlurCommandResponse merge(BlurCommandRequest request, List<BlurCommandResponse> responses)
      throws BlurException, IOException {
    Object fieldValue = request.getFieldValue();
    if (fieldValue instanceof AdhocByteCodeCommandRequest) {
      AdhocByteCodeCommandRequest commandRequest = request.getAdhocByteCodeCommandRequest();
      AdhocByteCodeCommandResponse commandResponse = merge(commandRequest, toAdhocByteCodeCommandResponses(responses));
      BlurCommandResponse response = new BlurCommandResponse();
      response.setAdhocByteCodeCommandResponse(commandResponse);
      return response;
    } else {
      throw new BException("Not implemented.");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private AdhocByteCodeCommandResponse merge(AdhocByteCodeCommandRequest commandRequest,
      List<AdhocByteCodeCommandResponse> adhocByteCodeCommandResponses) throws BlurException, IOException {
    Map<String, ByteBuffer> classData = commandRequest.getClassData();
    ClassLoader classLoader = CommandUtils.getClassLoader(classData);
    Object[] args = CommandUtils.getArgs(classLoader, commandRequest.getArguments());
    Command<?, ?> command = CommandUtils.toObjectViaSerialization(classLoader, commandRequest.getInstanceData());
    command.setArgs(args);
    List<?> results = getResults(classLoader, adhocByteCodeCommandResponses);
    Object r = command.mergeFinal((List) results);
    Value value = CommandUtils.toValue(r);
    AdhocByteCodeCommandResponse adhocByteCodeCommandResponse = new AdhocByteCodeCommandResponse();
    adhocByteCodeCommandResponse.setResult(value);
    return adhocByteCodeCommandResponse;
  }

  private List<?> getResults(ClassLoader classLoader, List<AdhocByteCodeCommandResponse> adhocByteCodeCommandResponses)
      throws BlurException, IOException {
    List<Object> result = new ArrayList<Object>();
    for (AdhocByteCodeCommandResponse response : adhocByteCodeCommandResponses) {
      Object object = CommandUtils.toObject(classLoader, response.getResult());
      result.add(object);
    }
    return result;
  }

  private List<AdhocByteCodeCommandResponse> toAdhocByteCodeCommandResponses(List<BlurCommandResponse> responses) {
    List<AdhocByteCodeCommandResponse> result = new ArrayList<AdhocByteCodeCommandResponse>();
    for (BlurCommandResponse r : responses) {
      result.add(r.getAdhocByteCodeCommandResponse());
    }
    return result;
  }

}
