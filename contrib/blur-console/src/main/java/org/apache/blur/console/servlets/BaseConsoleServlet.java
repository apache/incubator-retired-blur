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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

public abstract class BaseConsoleServlet extends HttpServlet {
	private static final long serialVersionUID = -5156028303476799953L;

	protected void sendError(HttpServletResponse response, Exception e) throws IOException {
		e.printStackTrace();
		String body = e.getMessage();
		response.setContentType("application/json");
		response.setContentLength(body.getBytes().length);
		response.setStatus(500);
		IOUtils.write(body, response.getOutputStream());
	}
	
	protected void sendGenericOk(HttpServletResponse response) throws IOException {
		response.setContentType("text/plain");
		response.setContentLength(6);
		response.setStatus(200);
		IOUtils.write("success", response.getOutputStream());
	}
}
