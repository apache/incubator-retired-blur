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
import org.apache.blur.metrics.BlurMetrics;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * Starts up a Jetty server to run the utility gui.
 * 
 */
public class HttpJettyServer {

  private static final Log LOG = LogFactory.getLog(HttpJettyServer.class);

  private Server server = null;

  /**
   * @param bindPort
   *          port of the process that the gui is wrapping
   * @param port
   *          port to run gui on
   * @param baseControllerPort
   *          ports that service runs on
   * @param baseShardPort
   * @param baseGuiShardPort
   *          port to run gui on
   * @param baseGuiControllerPort
   *          port to run gui on
   * @param base
   *          location of webapp to serve
   * @param bm
   *          metrics object for using.
   * @throws IOException
   */
  public HttpJettyServer(int bindPort, int port, int baseControllerPort, int baseShardPort, int baseGuiControllerPort, int baseGuiShardPort, String base, BlurMetrics bm)
      throws IOException {
    server = new Server(port);

    String logDir = System.getProperty("blur.logs.dir");
    String logFile = System.getProperty("blur.log.file");
    String blurLogFile = logDir + "/" + logFile;
    System.setProperty("blur.gui.servicing.port", bindPort + "");
    System.setProperty("blur.base.shard.port", baseShardPort + "");
    System.setProperty("blur.base.controller.port", baseControllerPort + "");
    System.setProperty("baseGuiShardPort", baseGuiShardPort + "");
    System.setProperty("baseGuiControllerPort", baseGuiControllerPort + "");
    System.setProperty("blur.gui.mode", base);
    LOG.info("System props:" + System.getProperties().toString());

    WebAppContext context = new WebAppContext();
    String warPath = getWarFolder();
    context.setWar(warPath);
    context.setContextPath("/");
    context.setParentLoaderPriority(true);
    context.addServlet(new ServletHolder(new LiveMetricsServlet()), "/livemetrics");
    context.addServlet(new ServletHolder(new MetricsServlet(bm)), "/metrics");
    context.addServlet(new ServletHolder(new LogServlet(blurLogFile)), "/logs");

    LOG.info("WEB GUI coming up for resource: " + base);
    LOG.info("WEB GUI thinks its at: " + warPath);
    LOG.info("WEB GUI log file being exposed: " + logDir == null ? "STDOUT" : blurLogFile);

    server.setHandler(context);

    try {
      server.start();
    } catch (Exception e) {
      throw new IOException("cannot start Http server for " + base, e);
    }
    LOG.info("WEB GUI up on port: " + port);
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

  private String getWarFolder() {
    String findBlurGuiInClassPath = findBlurGuiInClassPath();
    if (findBlurGuiInClassPath != null) {
      return findBlurGuiInClassPath;
    }
    String name = getClass().getName().replace('.', '/');
    String classResource = "/" + name + ".class";
    String pathToClassResource = getClass().getResource(classResource).toString();
    pathToClassResource = pathToClassResource.replace('/', File.separatorChar);
    int indexOfJar = pathToClassResource.indexOf(".jar");
    if (indexOfJar < 0) {
      int index = pathToClassResource.indexOf(name);
      String pathToClasses = pathToClassResource.substring(0, index);
      int indexOfProjectName = pathToClasses.indexOf("/blur-gui/");
      return pathToClasses.substring(0, indexOfProjectName) + "/blur-gui/src/main/webapp";
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

}
