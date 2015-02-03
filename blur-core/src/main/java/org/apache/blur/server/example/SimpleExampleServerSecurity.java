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

import java.lang.reflect.Method;
import java.net.InetAddress;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.server.ServerSecurityFilter;
import org.apache.blur.server.ServerSecurityFilterFactory;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.user.User;

public class SimpleExampleServerSecurity extends ServerSecurityFilterFactory {

  @Override
  public ServerSecurityFilter getServerSecurity(ServerType server, BlurConfiguration configuration) {
    return new ServerSecurityFilter() {
      @Override
      public boolean canAccess(Method method, Object[] args, User user, InetAddress address, int port) throws BlurException {
        if (method.getName().equals("createTable")) {
          if (user != null && user.getUsername().equals("admin")) {
            return true;
          }
          return false;
        }
        return true;
      }
    };
  }

}
