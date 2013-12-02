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
package org.apache.blur.trace;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.blur.BlurConfiguration;

public abstract class TraceStorage implements Closeable {

  protected final BlurConfiguration _configuration;

  public TraceStorage(BlurConfiguration configuration) {
    _configuration = configuration;
  }

  public abstract void store(TraceCollector collector);
  
  public abstract List<String> getTraceIds() throws IOException;
  
  public abstract List<String> getRequestIds(String traceId) throws IOException;
  
  public abstract String getRequestContentsJson(String traceId, String requestId) throws IOException;
  
  public abstract void removeTrace(String traceId) throws IOException;

}
