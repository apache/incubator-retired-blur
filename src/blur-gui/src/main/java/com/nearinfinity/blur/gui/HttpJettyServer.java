package com.nearinfinity.blur.gui;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

/**
 * Starts up a Jetty server to run the utility gui
 * @author gman
 *
 */
public class HttpJettyServer {
	
	private static final Log LOG = LogFactory.getLog(HttpJettyServer.class);

	private Server server = null;
	
	public HttpJettyServer(String port, String base) throws IOException  {
        server = new Server(Integer.parseInt(port));
        
        WebAppContext context = new WebAppContext();
        context.setWar(getJarFolder() + "../src/blur-gui/src/main/webapps/" + base);
        context.setContextPath("/");
        context.setParentLoaderPriority(true);
        
        LOG.info("WEB GUI coming up for resource: " + base);
        LOG.info("WEB GUI thinks its at: " + getJarFolder());
        
        server.setHandler(context);
 
        try {
			server.start();
		} catch (Exception e) {
			throw new IOException("cannot start Http server for " + base, e);
		}
        LOG.info("WEB GUI up on port: " + port);
	}
	
	  private String getJarFolder() {
		    String name = this.getClass().getName().replace('.', '/');
		    String s = this.getClass().getResource("/" + name + ".class").toString();
		    LOG.info("s: " + s);
		    s = s.replace('/', File.separatorChar);
		    LOG.info("s: " + s);
		    s = s.substring(0, s.indexOf(".jar")+4);
		    LOG.info("s: " + s);
		    s = s.substring(s.lastIndexOf(':')+1);
		    LOG.info("s: " + s);
		    return s.substring(0, s.lastIndexOf(File.separatorChar)+1);
		  } 
	
	public void close() {
		if(server != null)
			try {
				LOG.info("stopping web server");
				server.stop();
				LOG.info("stopped web server");
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
}
