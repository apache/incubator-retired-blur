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
package org.apache.blur.server.example;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.server.BlurServerContext;
import org.apache.blur.server.FilteredBlurServer;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.ErrorType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.thrift.generated.User;

public class DisableTableAdminServer extends FilteredBlurServer {

  private static final String SYSTEM = "system";

  public DisableTableAdminServer(BlurConfiguration configuration, Iface iface, boolean shard) {
    super(configuration, iface, shard);
  }

  @Override
  public void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {
    BlurServerContext serverContext = getServerContext();
    User user = serverContext.getUser();
    if (isAllowed(user)) {
      _iface.createTable(tableDescriptor);
    } else {
      throw new BlurException("Action not allowed by this user.", null, ErrorType.UNKNOWN);
    }
  }

  @Override
  public void enableTable(String table) throws BlurException, TException {
    BlurServerContext serverContext = getServerContext();
    User user = serverContext.getUser();
    if (isAllowed(user)) {
      _iface.enableTable(table);
    } else {
      throw new BlurException("Action not allowed by this user.", null, ErrorType.UNKNOWN);
    }
  }

  @Override
  public void disableTable(String table) throws BlurException, TException {
    BlurServerContext serverContext = getServerContext();
    User user = serverContext.getUser();
    if (isAllowed(user)) {
      _iface.disableTable(table);
    } else {
      throw new BlurException("Action not allowed by this user.", null, ErrorType.UNKNOWN);
    }
  }

  @Override
  public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
    BlurServerContext serverContext = getServerContext();
    User user = serverContext.getUser();
    if (isAllowed(user)) {
      _iface.removeTable(table, deleteIndexFiles);
    } else {
      throw new BlurException("Action not allowed by this user.", null, ErrorType.UNKNOWN);
    }
  }
  
  private boolean isAllowed(User user) {
    if (user == null) {
      return false;
    }
    String username = user.getUsername();
    if (username == null) {
      return false;
    }
    if (username.toLowerCase().equals(SYSTEM)) {
      return true;
    }
    return false;
  }

}
