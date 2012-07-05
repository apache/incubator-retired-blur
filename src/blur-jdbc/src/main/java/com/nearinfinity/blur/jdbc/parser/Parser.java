package com.nearinfinity.blur.jdbc.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

  private static final String SELECT = "select";
  private static final String WHERE = "where";
  private static final String FROM = "from";
  private static final String SEP = new String(new char[] { 1 });

  public static void main(String[] args) {
    // System.out.println(new
    // Parser().parse("select * from table where query('person.pn:(nice cool)')"));
    System.out.println(new Parser().parse("select * from table where person.pn = 'coandol''s' and jon='asdndandanda' And person.pf ='niorce' or nice = 'be'"));
    // System.out.println(new
    // Parser().parse("select id,locationid,score,cf1.* from table where query('+person.pn:(nice cool) AND cool.a:nice')"));
  }

  private String where;
  private String tableName;
  private List<String> columnNames;

  public Parser parse(String query) {
    columnNames = getColumnNames(query);
    tableName = getTableName(query);
    where = getWhere(query);
    return this;
  }

  private String getWhere(String query) {
    StringBuilder result = new StringBuilder();
    StringTokenizer tokenizer = new StringTokenizer(query);
    while (tokenizer.hasMoreTokens()) {
      if (WHERE.equals(tokenizer.nextToken().toLowerCase())) {
        while (tokenizer.hasMoreTokens()) {
          String token = tokenizer.nextToken();
          result.append(token).append(' ');
        }
      }
    }
    return getQuery(result.toString().trim());
  }

  private String getQuery(String query) {
    Pattern p = Pattern.compile("([qQ][uU][eE][rR][yY]\\s*\\(\\s*')(.*)('\\s*\\).*)");
    Matcher matcher = p.matcher(query);
    if (matcher.find()) {
      if (matcher.groupCount() != 3) {// first one is the whole string
        throw new RuntimeException("malformed query [" + query + "]");
      }
      return matcher.group(2);// 2nd group is the lucene query
    } else {
      return changeQueryToLucene(query);
    }
  }

  private String changeQueryToLucene(String query) {
    query = fixAndsOrs(query);
    // System.out.println(query);
    query = query.replaceAll("\\s*=\\s*", ":");
    // System.out.println(query);
    query = query.replace("''", SEP);
    // System.out.println(query);
    query = query.replaceAll("'", "");
    // System.out.println(query);
    query = query.replace(SEP, "'");
    // System.out.println(query);
    return query;
  }

  private String fixAndsOrs(String query) {
//    System.out.println(query);
    query = fixToUpperToken(query, "AND");
//    System.out.println(query);
    query = fixToUpperToken(query, "OR");
//    System.out.println(query);
    return query;
  }

  private String fixToUpperToken(String query, String token) {
    String queryUpper = query.toUpperCase();
    int start = 0;
    int index = queryUpper.indexOf(token, start);
    while (index != -1) {
      if (!query.substring(index, index + token.length()).equals(token)) {
        String everythingInStringToCurrentPosition = query.substring(0, index);
        if (!isHitInParameter(everythingInStringToCurrentPosition)) {
          query = query.substring(0, index) + token + query.substring(index + token.length());
          return fixToUpperToken(query, token);
        }
      }
      start = index + 1;
      index = queryUpper.indexOf(token, start);
    }
    return query;
  }

  private boolean isHitInParameter(String everythingInStringToCurrentPosition) {
    char[] charArray = everythingInStringToCurrentPosition.toCharArray();
    int count = 0;
    for (int i = 0; i < charArray.length; i++) {
      if (charArray[i] == '\'') {
        count++;
      }
    }
    return count % 2 != 0;
  }

  private String getTableName(String query) {
    StringTokenizer tokenizer = new StringTokenizer(query);
    while (tokenizer.hasMoreTokens()) {
      if (FROM.equals(tokenizer.nextToken().toLowerCase())) {
        if (tokenizer.hasMoreTokens()) {
          return tokenizer.nextToken();
        }
      }
    }
    throw new IllegalArgumentException("Table not found");
  }

  private List<String> getColumnNames(String query) {
    StringTokenizer tokenizer = new StringTokenizer(query);
    List<String> columnNames = new ArrayList<String>();
    while (tokenizer.hasMoreTokens()) {
      if (SELECT.equals(tokenizer.nextToken().toLowerCase())) {
        while (tokenizer.hasMoreTokens()) {
          String token = tokenizer.nextToken();
          if (FROM.equals(token)) {
            return columnNames;
          }
          processColumnToken(columnNames, token);
        }
      }
    }
    return null;
  }

  private void processColumnToken(List<String> columnNames, String token) {
    StringTokenizer tokenizer = new StringTokenizer(token, ",");
    while (tokenizer.hasMoreTokens()) {
      columnNames.add(tokenizer.nextToken());
    }
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public String getWhere() {
    if (where == null || where.trim().isEmpty()) {
      return "*";
    }
    return where;
  }

  @Override
  public String toString() {
    return "Parser [columnNames=" + columnNames + ", tableName=" + tableName + ", where=" + where + "]";
  }
}
