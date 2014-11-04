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
package org.apache.blur.manager.writer;

import java.io.IOException;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.lucene.util.InfoStream;

public class LoggingInfoStream extends InfoStream {

  private static final Log LOG = LogFactory.getLog("LUCENE_WRITER_INFO_STREAM");

  private final String _table;
  private final String _shard;

  public LoggingInfoStream(String table, String shard) {
    _table = table;
    _shard = shard;
  }

  @Override
  public void message(String component, String message) {
    LOG.info("Table [" + _table + "] Shard [" + _shard + "] Component [" + component + "] Message [" + message + "]");
  }

  @Override
  public boolean isEnabled(String component) {
    return LOG.isInfoEnabled();
  }

  @Override
  public void close() throws IOException {

  }

}
