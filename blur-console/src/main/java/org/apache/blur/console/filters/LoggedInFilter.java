package org.apache.blur.console.filters;
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
import org.apache.blur.console.model.User;
import org.apache.commons.io.IOUtils;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

public class LoggedInFilter implements Filter {

  private static final String UNAUTHORIZED = "User is unauthorized (not logged in)";
  private static final String FORBIDDEN = "User is forbidden from performing this action";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpSession session = request.getSession();
    String path = request.getServletPath();
    User user = (User) session.getAttribute("user");
    if(path.startsWith("/service/auth") || path.startsWith("/service/config.js") || user != null) {
      try {
        filterChain.doFilter(servletRequest, servletResponse);
      } catch(UnauthorizedException e) {
        sendUnauthorized((HttpServletResponse) servletResponse);
      } catch(ForbiddenException e) {
        sendForbidden((HttpServletResponse) servletResponse);
      }
    } else {
      sendUnauthorized((HttpServletResponse) servletResponse);
    }
  }

  protected void sendUnauthorized(HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    response.setContentLength(UNAUTHORIZED.getBytes().length);
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    IOUtils.write(UNAUTHORIZED, response.getOutputStream());
  }

  protected void sendForbidden(HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    response.setContentLength(FORBIDDEN.getBytes().length);
    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    IOUtils.write(FORBIDDEN, response.getOutputStream());
  }

  @Override
  public void destroy() {
  }
}
