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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.blur.manager.writer.BlurIndex;

public abstract class Command<T1, T2> implements Serializable {

  private static final long serialVersionUID = 1L;

  private Object[] _args;

  public Object[] getArgs() {
    return _args;
  }

  public void setArgs(Object[] args) {
    _args = args;
  }

  public abstract T2 mergeFinal(List<T2> results) throws IOException;

  public abstract T2 mergeIntermediate(List<T1> results) throws IOException;

  public abstract T1 call(BlurIndex blurIndex) throws IOException;

  public Callable<T1> createCallable(final BlurIndex blurIndex) {
    return new Callable<T1>() {
      @Override
      public T1 call() throws Exception {
        return Command.this.call(blurIndex);
      }
    };
  }

}
