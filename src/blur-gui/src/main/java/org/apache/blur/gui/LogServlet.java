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
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LogServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private String filePath = null;
  private int buffLen = 8192;

  public LogServlet(String filePath) {
    this.filePath = filePath;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    response.setContentType("text/html");
    PrintWriter out = response.getWriter();

    File f = new File(filePath);
    RandomAccessFile ram = new RandomAccessFile(f, "r");

    String offsetStr = request.getParameter("offset");
    long offset = -1;
    if (offsetStr != null)
      offset = Long.parseLong(offsetStr);

    long start = 0;
    long length = ram.length();
    // figure out buffer
    if (length < buffLen)
      buffLen = new Long(length).intValue();

    // use offset if passed in
    if (offset >= 0)
      start = offset;
    else
      start = length - buffLen;

    // calc new offset
    offset = start - buffLen;
    if (offset < 0)
      offset = 0;

    // buffer
    byte[] buff = new byte[buffLen];

    ram.seek(start);
    ram.read(buff);

    String returnStr = new String(buff, "UTF-8").replaceAll("\n", "\n<br>");

    out.write("<html><link href='style.css' rel='stylesheet' type='text/css' /><body>");
    out.write("<a href='index.html'>home</a><br/>");
    out.write("<p>File:<b> " + f.toString() + "</b> (" + start + "/" + length + ")</p>");
    if (start != 0) {
      out.write("<a href='logs?offset=" + 0 + "'>start</a>");
      out.write(" <a href='logs?offset=" + offset + "'>prev</a>");
    }
    if (start + buffLen < length) {
      out.write(" <a href='logs?offset=" + (start + buffLen) + "'>next</a>");
      out.write(" <a href='logs?offset=" + ((length - buffLen > 0) ? (length - buffLen) : 0) + "'>end</a>");
    }
    out.write("<br/>");
    out.write(returnStr);
    out.write("</body></html>");

    ram.close();
  }

}
