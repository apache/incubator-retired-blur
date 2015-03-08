package org.apache.blur.gui;

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
import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("serial")
public class LogsServlet extends HttpServlet {

  private final String _dir;

  public LogsServlet(String dir) {
    _dir = dir;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    try {
      processGet(request, response);
    } catch (JSONException e) {
      throw new IOException(e);
    }
  }

  private void processGet(HttpServletRequest request, HttpServletResponse response) throws JSONException, IOException {
    response.setContentType("text/html");
    listFiles(response);
  }

  private void listFiles(HttpServletResponse response) throws JSONException, IOException {
    JSONObject jsonObject = new JSONObject();
    File[] files = new File(_dir).listFiles();
    JSONArray array = new JSONArray();
    for (File file : files) {
      array.put(file.getName());
    }
    jsonObject.put("files", array);
    jsonObject.write(response.getWriter());
  }

}
