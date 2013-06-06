package org.apache.blur.thrift;

import static org.junit.Assert.*;

import org.junit.Test;

public class ConnectionTest {
  
  @Test
  public void testConnectionParsingWithOutProxy() {
    Connection connection = new Connection("host:1000");
    assertEquals("host",connection.getHost());
    assertEquals(1000,connection.getPort());
    assertNull(connection.getProxyHost());
    assertEquals(-1,connection.getProxyPort());
  }

  @Test
  public void testConnectionParsingWithProxy() {
    Connection connection = new Connection("host:1000/proxyhost:2000");
    assertEquals("host",connection.getHost());
    assertEquals(1000,connection.getPort());
    assertEquals("proxyhost",connection.getProxyHost());
    assertEquals(2000,connection.getProxyPort());
  }
}
