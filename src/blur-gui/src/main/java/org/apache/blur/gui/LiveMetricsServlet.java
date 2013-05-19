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
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class LiveMetricsServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  
  public LiveMetricsServlet() {}

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    /*
     * The live metrics page is currently not being used
     */
     
	/* 
	response.setContentType("application/json");
    PrintWriter out = response.getWriter();
    out.write("{");
    out.write("\"jvm\":{\"xLabel\":\"Time\",\"yLabel\":\"Heap (GB)\",\"lines\":");
    heapMetrics.writeJson(out);
    out.write("}");
    out.write(",\"blur_calls\":{\"xLabel\":\"Time\",\"yLabel\":\"Rates\",\"lines\":");
    queryMetrics.writeGraph1Json(out);
    out.write("}");
    out.write(",\"blur_recordRates\":{\"xLabel\":\"Time\",\"yLabel\":\"Rates\",\"lines\":");
    queryMetrics.writeGraph2Json(out);
    out.write("}");
    out.write(",\"system\":{\"xLabel\":\"Time\",\"yLabel\":\"Load\",\"lines\":");
    systemLoadMetrics.writeJson(out);
    out.write("}");
    out.write("}");
    */
  }

}
