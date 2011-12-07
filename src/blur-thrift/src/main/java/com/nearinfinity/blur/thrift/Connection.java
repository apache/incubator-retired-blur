package com.nearinfinity.blur.thrift;


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
    } else {
      throw new RuntimeException("Connection string of [" + connectionStr + "] does not match 'host1:port,host2:port,...'");
    }
  }
  
  public Connection(int port, String host, String proxyHost, int proxyPort) {
    _port = port;
    _host = host;
    _proxyHost = proxyHost;
    _proxyPort = proxyPort;
    _proxy = true;
  }

  public Connection(int port, String host) {
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
}
