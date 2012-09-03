package org.apache.blur.thrift;

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

public class Connection {

  private int _port;
  private String _host;
  private String _proxyHost;
  private int _proxyPort;
  private boolean _proxy = false;

  public Connection(String connectionStr) {
    int index = connectionStr.indexOf(':');
    if (index >= 0) {
      _host = connectionStr.substring(0, index);
      _port = Integer.parseInt(connectionStr.substring(index + 1));
      // @TODO make this connection parse proxy ports as well
    } else {
      throw new RuntimeException("Connection string of [" + connectionStr + "] does not match 'host1:port,host2:port,...'");
    }
  }

  public Connection(String host, int port, String proxyHost, int proxyPort) {
    _port = port;
    _host = host;
    _proxyHost = proxyHost;
    _proxyPort = proxyPort;
    _proxy = true;
  }

  public Connection(String host, int port) {
    _port = port;
    _host = host;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public boolean isProxy() {
    return _proxy;
  }

  public int getProxyPort() {
    return _proxyPort;
  }

  public String getProxyHost() {
    return _proxyHost;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_host == null) ? 0 : _host.hashCode());
    result = prime * result + _port;
    result = prime * result + (_proxy ? 1231 : 1237);
    result = prime * result + ((_proxyHost == null) ? 0 : _proxyHost.hashCode());
    result = prime * result + _proxyPort;
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
    Connection other = (Connection) obj;
    if (_host == null) {
      if (other._host != null)
        return false;
    } else if (!_host.equals(other._host))
      return false;
    if (_port != other._port)
      return false;
    if (_proxy != other._proxy)
      return false;
    if (_proxyHost == null) {
      if (other._proxyHost != null)
        return false;
    } else if (!_proxyHost.equals(other._proxyHost))
      return false;
    if (_proxyPort != other._proxyPort)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Connection [_host=" + _host + ", _port=" + _port + ", _proxy=" + _proxy + ", _proxyHost=" + _proxyHost + ", _proxyPort=" + _proxyPort + "]";
  }

  public Object getConnectionStr() {
    if (_proxyHost != null) {
      return _host + ":" + _port + "/" + _proxyHost + ":" + _proxyPort;
    }
    return _host + ":" + _port;
  }
}
