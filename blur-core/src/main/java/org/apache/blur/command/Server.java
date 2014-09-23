package org.apache.blur.command;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class Server extends Location<Server> implements Comparable<Server> {

  private final String _server;

  public Server(String server) {
    _server = server;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_server == null) ? 0 : _server.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Server other = (Server) obj;
    if (_server == null) {
      if (other._server != null)
        return false;
    } else if (!_server.equals(other._server))
      return false;
    return true;
  }

  public String getServer() {
    return _server;
  }

  @Override
  public int compareTo(Server o) {
    if (o == null) {
      return -1;
    }
    return _server.compareTo(o._server);
  }

  @Override
  public String toString() {
    return "Server [server=" + _server + "]";
  }

}
