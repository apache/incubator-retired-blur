package org.apache.blur.console.servlets;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.blur.console.util.HttpUtil;
import org.apache.blur.console.util.TableUtil;
import org.codehaus.jackson.map.ObjectMapper;

public class TablesServlet extends BaseConsoleServlet {
  private static final long serialVersionUID = -5725846390100596115L;
  private static Pattern tableSchemaPattern = Pattern.compile("/(.*)/schema");
  private static Pattern tableEnablePattern = Pattern.compile("/(.*)/enable");
  private static Pattern tableDisablePattern = Pattern.compile("/(.*)/disable");
  private static Pattern tableDeletePattern = Pattern.compile("/(.*)/delete");
  private static Pattern tableTermsPattern = Pattern.compile("/(.*)/(.*)/(.*)/terms");

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    String path = req.getPathInfo();
    Matcher m;
    if (path == null) {
      summary(res);
    } else if ((m = tableSchemaPattern.matcher(path)).matches()) {
      schema(res, m.group(1));
    } else if ((m = tableTermsPattern.matcher(path)).matches()) {
      terms(res, m.group(1), m.group(2), m.group(3), req.getParameter("startsWith"));
    } else if ((m = tableEnablePattern.matcher(path)).matches()) {
      enable(res, m.group(1));
    } else if ((m = tableDisablePattern.matcher(path)).matches()) {
      disable(res, m.group(1));
    } else if ((m = tableDeletePattern.matcher(path)).matches()) {
      delete(res, m.group(1), req.getParameter("includeFiles"));
    } else {
      sendNotFound(res, req.getRequestURI());
    }
  }

  @SuppressWarnings("rawtypes")
  private void summary(HttpServletResponse response) throws IOException {
    Map<String, List> tableSummaries = new HashMap<String, List>();
    try {
      tableSummaries = TableUtil.getTableSummaries();
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(response, e);
      return;
    }

    HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(tableSummaries), HttpUtil.JSON);
  }

  private void schema(HttpServletResponse response, String table) throws IOException {
    Object schema;
    try {
      schema = TableUtil.getSchema(table);
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(response, e);
      return;
    }

    HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(schema), HttpUtil.JSON);
  }

  private void terms(HttpServletResponse res, String table, String family, String column, String startsWith) throws IOException {
    List<String> terms = new ArrayList<String>();
    try {
      terms = TableUtil.getTerms(table, family, column, startsWith);
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(res, e);
      return;
    }

    HttpUtil.sendResponse(res, new ObjectMapper().writeValueAsString(terms), HttpUtil.JSON);
  }

  private void enable(HttpServletResponse response, String table) throws IOException {
    try {
      TableUtil.enableTable(table);
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(response, e);
      return;
    }
    sendGenericOk(response);
  }

  private void disable(HttpServletResponse response, String table) throws IOException {
    try {
      TableUtil.disableTable(table);
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(response, e);
      return;
    }
    sendGenericOk(response);
  }

  private void delete(HttpServletResponse response, String table, String includeFiles) throws IOException {
    try {
      TableUtil.deleteTable(table, Boolean.parseBoolean(includeFiles));
    } catch (IOException e) {
      throw new IOException(e);
    } catch (Exception e) {
      sendError(response, e);
      return;
    }
    sendGenericOk(response);
  }
}
