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
package org.apache.blur.jdbc.parser;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class ParserTest {

  @Test
  public void test1() {
    Parser parser = new Parser();
    parser.parse("select * from table1");
    assertEquals("table1", parser.getTableName());
    List<String> columnNames = parser.getColumnNames();
    assertEquals(1, columnNames.size());
    assertEquals("*", columnNames.get(0));
    assertEquals("*", parser.getWhere());
  }

  @Test
  public void test2() {
    Parser parser = new Parser();
    parser.parse("select * from 'table1'");
    assertEquals("table1", parser.getTableName());
    List<String> columnNames = parser.getColumnNames();
    assertEquals(1, columnNames.size());
    assertEquals("*", columnNames.get(0));
    assertEquals("*", parser.getWhere());
  }

  @Test
  public void test3() {
    Parser parser = new Parser();
    parser.parse("select tbl.* from 'table1' tbl");
    assertEquals("table1", parser.getTableName());
    List<String> columnNames = parser.getColumnNames();
    assertEquals(1, columnNames.size());
    assertEquals("*", columnNames.get(0));
    assertEquals("*", parser.getWhere());
  }

}
