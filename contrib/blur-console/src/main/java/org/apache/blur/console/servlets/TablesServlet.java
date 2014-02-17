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
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.blur.console.util.HttpUtil;
import org.apache.blur.console.util.TableUtil;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

public class TablesServlet extends HttpServlet {
	private static final long serialVersionUID = -5725846390100596115L;
	private static Pattern tableSchemaPattern = Pattern.compile("/(.*)/schema");
	private static Pattern tableEnablePattern = Pattern.compile("/(.*)/enable");
	private static Pattern tableDisablePattern = Pattern.compile("/(.*)/disable");
	private static Pattern tableDeletePattern = Pattern.compile("/(.*)/delete");
	private static Pattern tableTermsPattern = Pattern.compile("/(.*)/(.*)/(.*)/terms");

	@SuppressWarnings("unchecked")
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String path = request.getPathInfo();
		Matcher m;
		if ("/summaries".equalsIgnoreCase(path)) {
			sendSummaries(response);
		} else if ((m = tableSchemaPattern.matcher(path)).matches()) {
			sendSchema(response, m.group(1));
		} else if ((m = tableTermsPattern.matcher(path)).matches()) {
			sendTerms(response, m.group(1), m.group(2), m.group(3), request.getParameterMap());
		} else if ((m = tableEnablePattern.matcher(path)).matches()) {
			enableTable(response, m.group(1));
		} else if ((m = tableDisablePattern.matcher(path)).matches()) {
			disableTable(response, m.group(1));
		} else if ((m = tableDeletePattern.matcher(path)).matches()) {
			deleteTable(response, m.group(1), request.getParameterMap());
		} else {
			response.setStatus(404);
			IOUtils.write("Route [" + path + "] doesn't exist", response.getOutputStream());
		}
	}
	
	private void sendError(HttpServletResponse response, Exception e) throws IOException {
		String body = e.getMessage();
		response.setContentType("application/json");
		response.setContentLength(body.getBytes().length);
		response.setStatus(500);
		IOUtils.write(body, response.getOutputStream());
	}
	
	private void sendGenericOk(HttpServletResponse response) throws IOException {
		response.setContentType("text/plain");
		response.setContentLength(6);
		response.setStatus(200);
		IOUtils.write("success", response.getOutputStream());
	}
	
	private void sendSummaries(HttpServletResponse response) throws IOException {
		Map<String, Object> tableSummaries = new HashMap<String, Object>();
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
	
	private void sendSchema(HttpServletResponse response, String table) throws IOException{
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
	
	private void sendTerms(HttpServletResponse response, String table, String family, String column, Map<String, String[]> params) throws IOException {
		List<String> terms = new ArrayList<String>();
		try {
			String startWith = null;
			if (params.containsKey("startWith")) {
				startWith = params.get("startWith")[0];
			}
			
			terms = TableUtil.getTerms(table, family, column, startWith);
		} catch (IOException e) {
			throw new IOException(e);
		} catch (Exception e) {
			sendError(response, e);
			return;
		}
		
		HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(terms), HttpUtil.JSON);
	}
	
	private void enableTable(HttpServletResponse response, String table) throws IOException {
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
	
	private void disableTable(HttpServletResponse response, String table) throws IOException {
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
	
	private void deleteTable(HttpServletResponse response, String table, Map<String, String[]> params) throws IOException {
		try {
			String includeFiles = null;
			if (params.containsKey("includeFiles")) {
				includeFiles = params.get("includeFiles")[0];
			}
			
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
