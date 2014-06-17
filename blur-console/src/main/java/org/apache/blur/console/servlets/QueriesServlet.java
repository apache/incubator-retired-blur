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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.blur.console.util.HttpUtil;
import org.apache.blur.console.util.QueryUtil;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;

public class QueriesServlet extends BaseConsoleServlet {
	private static final long serialVersionUID = -5725846390100596115L;
	private static Pattern queryCancelPattern = Pattern.compile("/(.*)/cancel");

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String path = request.getPathInfo();
		Matcher m;
		if (path == null) {
			sendQueries(response);
		} else if ("/performance".equalsIgnoreCase(path)) {
			sendCurrentQueryCount(response);
		} else if ((m = queryCancelPattern.matcher(path)).matches()) {
			cancelQuery(response, m.group(1), request.getParameter("table"));
		} else {
			response.setStatus(404);
			IOUtils.write("Route [" + path + "] doesn't exist", response.getOutputStream());
		}
	}
	
	private void sendCurrentQueryCount(HttpServletResponse response) throws IOException {
		int count;
		try {
			count = QueryUtil.getCurrentQueryCount();
		} catch (IOException e) {
			throw new IOException(e);
		} catch (Exception e) {
			sendError(response, e);
			return;
		}
		
		HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(count), HttpUtil.JSON);
	}
	
	private void sendQueries(HttpServletResponse response) throws IOException {
		Map<String, Object> queries = new HashMap<String, Object>();
		try {
			queries = QueryUtil.getQueries();
		} catch (IOException e) {
			throw new IOException(e);
		} catch (Exception e) {
			sendError(response, e);
			return;
		}
		
		HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(queries), HttpUtil.JSON);
	}
	
	private void cancelQuery(HttpServletResponse response, String uuid, String table) throws IOException {
		try {
			QueryUtil.cancelQuery(table, uuid);
		} catch (IOException e) {
			throw new IOException(e);
		} catch (Exception e) {
			sendError(response, e);
			return;
		}
		
		sendGenericOk(response);
	}
}
