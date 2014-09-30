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
package org.apache.blur.command;

import java.io.IOException;
import java.util.Map;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Blur.Iface;

public abstract class ClusterReadCombiningCommand<T> extends Command<T> implements IndexReadCombining<T, T> {

  public abstract T execute(IndexContext context) throws IOException, InterruptedException;

  public abstract T combine(CombiningContext context, Map<? extends Location<?>, T> results) throws IOException,
      InterruptedException;

  @Override
  public T run(Args arguments) throws IOException {
    try {
      return CommandRunner.run(this, arguments);
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public T run(Args arguments, String connectionStr) throws IOException {
    try {
      return CommandRunner.run(this, arguments, connectionStr);
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }
  @Override
  public T run(Args arguments, Iface client) throws IOException {
    try {
      return CommandRunner.run(this, arguments, client);
    } catch (BlurException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  } 
}
