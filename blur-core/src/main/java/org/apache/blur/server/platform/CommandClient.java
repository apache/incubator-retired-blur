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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandRequest;
import org.apache.blur.thrift.generated.AdhocByteCodeCommandResponse;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurCommandRequest;
import org.apache.blur.thrift.generated.BlurCommandResponse;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Value;
import org.apache.commons.io.IOUtils;

public class CommandClient {

  private static final Log LOG = LogFactory.getLog(CommandClient.class);

  private final Iface _client;

  public CommandClient(Iface client) {
    _client = client;
  }

  public <T1, T2> T2 execute(String table, Command<T1, T2> command) throws BlurException, TException, IOException {
    Set<String> tables = new HashSet<String>();
    tables.add(table);
    return execute(tables, new Object[] {}, command);
  }

  public <T1, T2> T2 execute(Set<String> tables, Object[] args, Command<T1, T2> command) throws BlurException,
      TException, IOException {
    BlurCommandRequest request = new BlurCommandRequest();
    AdhocByteCodeCommandRequest adhocByteCodeCommandRequest = new AdhocByteCodeCommandRequest();
    packCommandAndClasses(adhocByteCodeCommandRequest, args, command);
    request.setAdhocByteCodeCommandRequest(adhocByteCodeCommandRequest);
    BlurCommandResponse blurCommandResponse = _client.execute(request);
    AdhocByteCodeCommandResponse response = blurCommandResponse.getAdhocByteCodeCommandResponse();
    Value result = response.getResult();
    return CommandUtils.toObject(getClass().getClassLoader(), result);
  }

  private void packCommandAndClasses(AdhocByteCodeCommandRequest request, Object[] args, Object command)
      throws IOException {
    request.setClassData(getClassData(command.getClass()));
    request.setInstanceData(CommandUtils.toBytesViaSerialization(command));
    request.setArguments(getArgs(args));
  }

  private List<Value> getArgs(Object[] args) throws IOException {
    List<Value> values = new ArrayList<Value>();
    for (Object o : args) {
      if (o == null) {
        values.add(null);
      } else {
        values.add(CommandUtils.toValue(o));
      }
    }
    return values;
  }

  private Map<String, ByteBuffer> getClassData(Class<?> clazz) throws IOException {
    String name = clazz.getName();
    String r = "/" + name.replace('.', '/') + ".class";
    URL url = getClass().getResource(r);
    Map<String, ByteBuffer> map = new HashMap<String, ByteBuffer>();
    packClassInfo(url, r, map);
    return map;
  }

  private void packClassInfo(URL url, String baseName, Map<String, ByteBuffer> map) throws IOException {
    try {
      URI uri = url.toURI();
      String path = uri.toString();
      LOG.info("Using path [{0}] to find class data to pack.", path);
      if (path.endsWith(baseName)) {
        String root = path.replace(baseName, "");
        URI rootUri = new URI(root);
        if (rootUri.getScheme().equals("file")) {
          File file = new File(rootUri);
          pack(rootUri, root, file, map);
          return;
        }
      }
      throw new IOException("Something bad has happened url [" + url + "] baseName [" + baseName + "]");
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

  }

  private void pack(URI rootUri, String root, File file, Map<String, ByteBuffer> map) throws IOException {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        pack(rootUri, root, f, map);
      }
    } else {
      URI uri = file.toURI();
      String classUri = uri.toString();
      String classResourcePath = classUri.replace(root, "");
      InputStream inputStream = getClass().getResourceAsStream(classResourcePath);
      byte[] classData = IOUtils.toByteArray(inputStream);
      inputStream.close();
      String className = getClassName(classResourcePath);
      LOG.info("Packing class [{0}] at uri [{1}].", className, classUri);
      map.put(className, ByteBuffer.wrap(classData));
    }
  }

  private String getClassName(String classResourcePath) {
    return classResourcePath.substring(1).replace(".class", "").replace('/', '.');
  }
}
