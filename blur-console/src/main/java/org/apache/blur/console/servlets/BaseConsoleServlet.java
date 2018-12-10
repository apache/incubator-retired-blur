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

import org.apache.blur.console.filters.ForbiddenException;
import org.apache.blur.console.filters.UnauthorizedException;
import org.apache.blur.console.model.User;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public abstract class BaseConsoleServlet extends HttpServlet {
  private static final long serialVersionUID = -5156028303476799953L;
  private static final Log log = LogFactory.getLog(BaseConsoleServlet.class);

  protected void sendError(HttpServletResponse response, Exception e) throws IOException {
    log.error("Error processing request.", e);
    String body = "Error processing request";
    if(e != null && e.getMessage() != null) {
      body = e.getMessage();
    }
    response.setContentType("application/json");
    response.setContentLength(body.getBytes().length);
    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    IOUtils.write(body, response.getOutputStream());
  }

  protected void sendGenericOk(HttpServletResponse response) throws IOException {
    String responseBody = "success";
    response.setContentType("text/plain");
    response.setContentLength(responseBody.getBytes().length);
    response.setStatus(HttpServletResponse.SC_OK);
    IOUtils.write(responseBody, response.getOutputStream());
  }

  protected void sendNotFound(HttpServletResponse response, String path) throws IOException {
    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    IOUtils.write("URL [" + path + "] doesn't exist", response.getOutputStream());
  }

  protected void authorize(HttpServletRequest request, String... roles) {
    User user = currentUser(request);
    for(String role: roles) {
      if(user.hasRole(role)){
        return;
      }
    }
    throw new ForbiddenException();
  }

  protected User currentUser(HttpServletRequest request) {
    HttpSession session = request.getSession();
    User user = (User) session.getAttribute("user");
    if(user == null) {
      throw new UnauthorizedException();
    }
    return user;
  }
}
