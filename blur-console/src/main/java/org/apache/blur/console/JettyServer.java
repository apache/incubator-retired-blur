package org.apache.blur.console;

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

import org.apache.blur.console.filters.LoggedInFilter;
import org.apache.blur.console.servlets.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.*;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.servlet.DispatcherType;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class JettyServer {
  private int port;
  private Server server;
  private boolean devMode;

  private static final String DEV_WEBAPPDIR = "src/main/webapp/public/";
  private static final String PROD_WEBAPPDIR = "webapp/";
  private static final String CONTEXTPATH = "/console";

  private final Log log = LogFactory.getLog(JettyServer.class);

  public JettyServer(int port, boolean devMode) {
    this.port = port;
    this.devMode = devMode;
  }

  public JettyServer start() throws MalformedURLException {
    createServer();
    return this;
  }

  public void join() {
    try {
      server.join();
    } catch (InterruptedException e) {
      log.info("Server shutting down");
    }
  }

  private void createServer() throws MalformedURLException {
    server = new Server(port);

    // for localhost:port/console/index.html and whatever else is in the webapp directory
    URL warUrl = null;
      if (devMode) {
          warUrl = new URL("file://" + new File(DEV_WEBAPPDIR).getAbsolutePath());
      } else {
          warUrl = this.getClass().getClassLoader().getResource(PROD_WEBAPPDIR);
      }
    String warUrlString = warUrl.toExternalForm();
    WebAppContext staticContext = new WebAppContext(warUrlString, CONTEXTPATH);


    // service calls
    ContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
    servletContext.setContextPath("/console/service");
    ServletHandler serviceHandler = new ServletHandler();
    serviceHandler.addServletWithMapping(AuthServlet.class, "/auth/*");
    serviceHandler.addServletWithMapping(NodesServlet.class, "/nodes/*");
    serviceHandler.addServletWithMapping(TablesServlet.class, "/tables/*");
    serviceHandler.addServletWithMapping(QueriesServlet.class, "/queries/*");
    serviceHandler.addServletWithMapping(SearchServlet.class, "/search/*");
    serviceHandler.addServletWithMapping(JavascriptServlet.class, "/config.js");
    serviceHandler.addFilterWithMapping(LoggedInFilter.class, "/*", FilterMapping.REQUEST);
    servletContext.setHandler(serviceHandler);


    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[] { servletContext, staticContext  });

    server.setHandler(handlers);
    System.out.println("started server on http://localhost:" + port + CONTEXTPATH);
    try {
      server.start();
    } catch (Exception e) {
      log.error("Error starting Blur Console Jetty Server.  Exiting", e);
      System.exit(1);
    }
  }
}
