package org.apache.blur.thrift;

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
import static org.junit.Assert.*;

import org.junit.Test;

public class ConnectionTest {

  @Test
  public void testConnectionParsingWithOutProxy() {
    Connection connection = new Connection("host:1000");
    assertEquals("host", connection.getHost());
    assertEquals(1000, connection.getPort());
    assertNull(connection.getProxyHost());
    assertEquals(-1, connection.getProxyPort());
  }

  @Test
  public void testConnectionParsingWithProxy() {
    Connection connection = new Connection("host:1000/proxyhost:2000");
    assertEquals("host", connection.getHost());
    assertEquals(1000, connection.getPort());
    assertEquals("proxyhost", connection.getProxyHost());
    assertEquals(2000, connection.getProxyPort());
  }
}
