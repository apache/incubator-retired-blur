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

import org.apache.blur.console.model.User;
import org.apache.blur.console.providers.IAuthenticationProvider;
import org.apache.blur.console.util.Config;
import org.apache.blur.console.util.HttpUtil;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AuthServlet extends BaseConsoleServlet {
  private static final String LOGIN_STATUS_FIELD = "loggedIn";
  private static final String LOGIN_FORM_FIELD = "formHtml";
  private static final String USER_FIELD = "user";

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String path = req.getPathInfo();
    if ("/login".equalsIgnoreCase(path)) {
      loginUser(req, resp);
    } else {
      sendNotFound(resp, req.getRequestURI());
    }
  }

  private void loginUser(HttpServletRequest request, HttpServletResponse response) throws IOException {
    Map<String, Object> responseData = new HashMap<String, Object>();
    HttpSession session = request.getSession();
    User user = (User) session.getAttribute("user");
    IAuthenticationProvider authenticationProvider = Config.getAuthenticationProvider();
    if(user == null) {
      user = authenticationProvider.login(request);
    }
    if (user == null) {
      responseData.put(LOGIN_STATUS_FIELD, false);
      String form = authenticationProvider.getLoginForm();
      if (form != null) {
        responseData.put(LOGIN_FORM_FIELD, form);
      }
    } else {
      Config.getAuthorizationProvider().setUserSecurityAttributes(user);
      responseData.put(LOGIN_STATUS_FIELD, true);
      responseData.put(USER_FIELD, user);
      session.setAttribute("user", user);
    }

    HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(responseData), HttpUtil.JSON);
  }
}
