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
package org.apache.blur.server;

import java.net.SocketAddress;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thirdparty.thrift_0_9_0.server.ServerContext;
import org.apache.blur.thrift.generated.User;
import org.apache.blur.thrift.server.ThriftTrace;
import org.apache.blur.thrift.server.ThriftTracer;

public class BlurServerContext implements ServerContext, ThriftTrace {

  private static final Log LOG = LogFactory.getLog(BlurServerContext.class);

  private User _user;
  private final SocketAddress _localSocketAddress;
  private final SocketAddress _remoteSocketAddress;
  private final String _localConnectionString;
  private final String _remoteConnectionString;
  private String _traceRootId;
  private String _traceRequestId;

  public BlurServerContext(SocketAddress localSocketAddress, SocketAddress remoteSocketAddress) {
    _localSocketAddress = localSocketAddress;
    _remoteSocketAddress = remoteSocketAddress;
    if (_localSocketAddress != null) {
      _localConnectionString = _localSocketAddress.toString();
    } else {
      _localConnectionString = null;
    }
    if (_remoteSocketAddress != null) {
      _remoteConnectionString = _remoteSocketAddress.toString();
    } else {
      _remoteConnectionString = null;
    }
  }

  public void setUser(User user) {
    LOG.debug("User [{0}] for context [{1}]", user, this);
    _user = user;
  }

  public User getUser() {
    return _user;
  }

  public String getTraceRootId() {
    return _traceRootId;
  }

  public String getTraceRequestId() {
    return _traceRequestId;
  }

  public void setTraceRootId(String traceRootId) {
    _traceRootId = traceRootId;
  }

  public void setTraceRequestId(String traceRequestId) {
    _traceRequestId = traceRequestId;
  }

  public SocketAddress getLocalSocketAddress() {
    return _localSocketAddress;
  }

  public SocketAddress getRemoteSocketAddress() {
    return _remoteSocketAddress;
  }

  public String getConnectionString(String sep) {
    return _localConnectionString + sep + _remoteConnectionString;
  }

  public void resetTraceIds() {
    _traceRootId = null;
    _traceRequestId = null;
  }

  @Override
  public ThriftTracer getTracer(String name) {
    return ThriftTracer.NOTHING;
  }

}
