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
import java.util.Properties;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.reporting.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Starts up a Jetty server to run the utility gui.
 */
public class HttpJettyServer {

  private static final Log LOG = LogFactory.getLog(HttpJettyServer.class);

  private Server server = null;

  private WebAppContext context;

  private int _localPort;

  /**
   * @param port
   *          port to run gui on
   * @throws IOException
   */
  public HttpJettyServer(Class<?> c, int port) throws IOException {
    server = new Server(port);
    String logDir = System.getProperty("blur.logs.dir");
    String logFile = System.getProperty("blur.log.file");
    String blurLogFile = logDir + "/" + logFile;
    LOG.info("System props:" + System.getProperties().toString());

    context = new WebAppContext();
    String warPath = getWarFolder(c);
    context.setWar(warPath);
    context.setContextPath("/");
    context.setParentLoaderPriority(true);
    context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
    context.addServlet(new ServletHolder(new LogServlet(blurLogFile)), "/logs");

    LOG.info("Http server thinks its at: " + warPath);
    LOG.info("Http server log file being exposed: " + logDir == null ? "STDOUT" : blurLogFile);

    server.setHandler(context);

    try {
      server.start();
    } catch (Exception e) {
      try {
        server.stop();
      } catch (Exception ex) {
        LOG.error("Unknown error while trying to stop server during error on startup.", ex);
      }
      throw new IOException("Cannot start Http server.", e);
    }
    for (int i = 0; i < 100; i++) {
      if (server.isRunning()) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        return;
      }
    }
    Connector[] connectors = server.getConnectors();
    for (Connector connector : connectors) {
      _localPort = ((ServerConnector)(connector)).getLocalPort();
    }
    LOG.info("Http server up on port: " + _localPort);
  }

  public WebAppContext getContext() {
    return context;
  }

  private static String findBlurGuiInClassPath() {
    Properties properties = System.getProperties();
    String cp = (String) properties.get("java.class.path");
    String[] split = cp.split(":");
    for (String s : split) {
      if (s.endsWith(".war")) {
        return s;
      }
    }
    return null;
  }

  private String getWarFolder(Class<?> c) {
    String findBlurGuiInClassPath = findBlurGuiInClassPath();
    if (findBlurGuiInClassPath != null) {
      return findBlurGuiInClassPath;
    }
    String name = c.getName().replace('.', '/');
    String classResource = "/" + name + ".class";
    String pathToClassResource = c.getResource(classResource).toString();
    pathToClassResource = pathToClassResource.replace('/', File.separatorChar);
    int indexOfJar = pathToClassResource.indexOf(".jar");
    if (indexOfJar < 0) {
      int index = pathToClassResource.indexOf(name);
      String pathToClasses = pathToClassResource.substring(0, index);
      int indexOfProjectName = pathToClasses.indexOf("/target/");
      String str = pathToClasses.substring(0, indexOfProjectName) + "/src/main/webapp";
      return str;
    }
    return null;
  }

  public void close() {
    if (server != null) {
      try {
        LOG.info("stopping web server");
        server.stop();
        LOG.info("stopped web server");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public int getLocalPort() {
    return _localPort;
  }

}
