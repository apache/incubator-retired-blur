package com.nearinfinity.blur.thrift;


public class Connection {
  
  private int _port;
  private String _host;
  private String _proxyHost;
  private int _proxyPort;
  private boolean _proxy = false;

  public Connection(String connectionStr) {
    int index = connectionStr.indexOf(':');
    _host = connectionStr.substring(0, index);
    _port = Integer.parseInt(connectionStr.substring(index + 1));
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
}
