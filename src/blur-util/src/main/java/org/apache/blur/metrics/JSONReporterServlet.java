package org.apache.blur.metrics;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class JSONReporterServlet extends HttpServlet {
  private static final long serialVersionUID = -3086441832701983642L;

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("application/json");
    PrintWriter writer = response.getWriter();
    JSONReporter.writeJSONData(writer);
    writer.flush();
  }

}
