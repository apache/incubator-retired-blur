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
import org.apache.blur.console.providers.IProvider;
import org.apache.blur.console.util.Config;
import org.apache.blur.console.util.HttpUtil;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AuthServlet extends BaseConsoleServlet {
  private static final String LOGIN_STATUS_FIELD = "loggedIn";
  private static final String LOGIN_FIELDS_FIELD = "requiredFields";
  private static final String LOGIN_RETRY_ALLOWED = "retryAllowed";
  private static final String AUTH_TOKEN = "authToken";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String path = req.getPathInfo();

    if (path == null) {
      checkCurrentAuth(req, resp);
    } else {
      sendNotFound(resp, req.getRequestURI());
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String path = req.getPathInfo();

    if ("login".equalsIgnoreCase(path)) {
      loginUser(req, resp);
    } else {
      sendNotFound(resp, req.getRequestURI());
    }
  }

  private void checkCurrentAuth(HttpServletRequest request, HttpServletResponse response) throws IOException {
    IProvider provider = Config.getProvider();
    Map<String, Object> responseData = new HashMap<String, Object>();

    if (provider.isLoginRequired()) {
      String authToken = HttpUtil.getFirstParam((String[]) request.getParameterMap().get(AUTH_TOKEN));

      if (StringUtils.isNotBlank(authToken) && provider.getUser(authToken, request) != null) {
        responseData.put(LOGIN_STATUS_FIELD, true);
      } else {
        responseData.put(LOGIN_STATUS_FIELD, false);
        responseData.put(LOGIN_FIELDS_FIELD, provider.getLoginFields());
      }
    } else {
      responseData.put(LOGIN_STATUS_FIELD, true);
    }

    HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(responseData), HttpUtil.JSON);
  }

  private void loginUser(HttpServletRequest request, HttpServletResponse response) throws IOException {
    Map<String, Object> data = new HashMap<String, Object>();

    IProvider provider = Config.getProvider();

    User user = provider.login(request);

    if (user == null) {
      data.put(LOGIN_STATUS_FIELD, false);

      boolean retry = provider.isRetryAllowed();
      data.put(LOGIN_RETRY_ALLOWED, retry);
      if (retry) {
        data.put(LOGIN_FIELDS_FIELD, provider.getLoginFields());
      }
    } else {
      data.put(AUTH_TOKEN, user.getAuthToken());
      data.put("roles", user.getRoles());
    }

    HttpUtil.sendResponse(response, new ObjectMapper().writeValueAsString(data), HttpUtil.JSON);
  }
}
