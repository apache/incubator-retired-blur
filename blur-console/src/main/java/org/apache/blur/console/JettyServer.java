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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.blur.console.filters.LoggedInFilter;
import org.apache.blur.console.servlets.AuthServlet;
import org.apache.blur.console.servlets.JavascriptServlet;
import org.apache.blur.console.servlets.NodesServlet;
import org.apache.blur.console.servlets.QueriesServlet;
import org.apache.blur.console.servlets.SearchServlet;
import org.apache.blur.console.servlets.TablesServlet;
import org.apache.blur.console.util.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

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
    
    if (Boolean.parseBoolean(Config.getBlurConfig().get("blur.console.ssl.enable", "false"))) {
    	SslContextFactory factory = new SslContextFactory(Boolean.parseBoolean(Config.getBlurConfig().get("blur.console.ssl.hostname.match", "true")));
    	factory.setKeyStorePath(Config.getBlurConfig().get("blur.console.ssl.keystore.path"));
    	factory.setKeyStorePassword(Config.getBlurConfig().get("blur.console.ssl.keystore.password"));
    	factory.setTrustStore(Config.getBlurConfig().get("blur.console.ssl.truststore.path"));
    	factory.setTrustStorePassword(Config.getBlurConfig().get("blur.console.ssl.truststore.password"));
    	
    	SslSelectChannelConnector sslConnector = new SslSelectChannelConnector(factory);
    	sslConnector.setPort(port);
    	
    	server.addConnector(sslConnector);
    }

    // for localhost:port/console/index.html and whatever else is in the webapp directory
    URL warUrl = null;
      if (devMode) {
          warUrl = new URL("file://" + new File(DEV_WEBAPPDIR).getAbsolutePath());
      } else {
          warUrl = this.getClass().getClassLoader().getResource(PROD_WEBAPPDIR);
      }
    String warUrlString = warUrl.toExternalForm();
    WebAppContext staticContext = new WebAppContext(warUrlString, CONTEXTPATH);
    staticContext.setSessionHandler(new SessionHandler());

    // service calls
//    ContextHandler servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
//    servletContext.setContextPath("/console/service");
    ServletHandler serviceHandler = new ServletHandler();
    serviceHandler.addServletWithMapping(AuthServlet.class, "/service/auth/*");
    serviceHandler.addServletWithMapping(NodesServlet.class, "/service/nodes/*");
    serviceHandler.addServletWithMapping(TablesServlet.class, "/service/tables/*");
    serviceHandler.addServletWithMapping(QueriesServlet.class, "/service/queries/*");
    serviceHandler.addServletWithMapping(SearchServlet.class, "/service/search/*");
    serviceHandler.addServletWithMapping(JavascriptServlet.class, "/service/config.js");
    serviceHandler.addFilterWithMapping(LoggedInFilter.class, "/service/*", FilterMapping.REQUEST);
//    servletContext.setHandler(serviceHandler);
    staticContext.setServletHandler(serviceHandler);


//    ContextHandlerCollection handlers = new ContextHandlerCollection();
//    handlers.setHandlers(new Handler[] { /*servletContext,*/ staticContext  });

    server.setHandler(staticContext);
    System.out.println("started server on http://localhost:" + port + CONTEXTPATH);
    try {
      server.start();
      System.out.println(server.getHandlers()[0]);
    } catch (Exception e) {
      log.error("Error starting Blur Console Jetty Server.  Exiting", e);
      System.exit(1);
    }
  }
}
