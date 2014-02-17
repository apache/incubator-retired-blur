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

import java.net.URL;

import org.apache.blur.console.servlets.DashboardServlet;
import org.apache.blur.console.servlets.QueriesServlet;
import org.apache.blur.console.servlets.TablesServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

public class JettyServer {
	private final static String WEBAPPDIR = "org/apache/blur/console/webapp";
	private final static String CONTEXTPATH = "/console";
	private final Log log = LogFactory.getLog(JettyServer.class);
		
	private int port;
	private Server server;
	
	public JettyServer(int port) {
		this.port = port;
	}
	
	public JettyServer start() {
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
	
	private void createServer() {
		server = new Server(port);
		
		// for localhost:port/console/index.html and whatever else is in the webapp directory
	    final URL warUrl = this.getClass().getClassLoader().getResource(WEBAPPDIR);
	    final String warUrlString = warUrl.toExternalForm();
	    server.setHandler(new WebAppContext(warUrlString, CONTEXTPATH));
	      
	    // for localhost:port/service/dashboard, etc.
	    final Context context = new Context(server, "/service", Context.SESSIONS);
	    context.addServlet(new ServletHolder(new DashboardServlet()), "/dashboard/*");
	    context.addServlet(new ServletHolder(new TablesServlet()), "/tables/*");
	    context.addServlet(new ServletHolder(new QueriesServlet()), "/queries/*");
	      
	    try {
			server.start();
		} catch (Exception e) {
			log.error("Error starting Blur Console Jetty Server.  Exiting", e);
			System.exit(1);
		}
	}
}
