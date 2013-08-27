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
