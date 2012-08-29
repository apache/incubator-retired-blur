package com.nearinfinity.blur.gui;

import java.io.File;
import java.io.IOException;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.metrics.BlurMetrics;

/**
 * Starts up a Jetty server to run the utility gui
 * @author gman
 *
 */
public class HttpJettyServer {
	
	private static final Log LOG = LogFactory.getLog(HttpJettyServer.class);

	private Server server = null;
	
	/**
	 * @param bindPort port of the process that the gui is wrapping
	 * @param port port to run gui on
	 * @param baseControllerPort ports that service runs on
	 * @param baseShardPort
	 * @param baseGuiShardPort port to run gui on
	 * @param baseGuiControllerPort port to run gui on
	 * @param base location of webapp to serve
	 * @param bm metrics object for using.
	 * @throws IOException
	 */
	//TODO: this got ugly, move to a set/start pattern
	public HttpJettyServer(int bindPort, int port, int baseControllerPort,
			int baseShardPort, int baseGuiControllerPort, int baseGuiShardPort,
			String base, BlurMetrics bm) throws IOException {
        server = new Server(port);
        
        String blurLogFile = System.getProperty("blur.logs.dir") + "/" + System.getProperty("blur.log.file");
        System.setProperty("blur.gui.servicing.port", bindPort+"");
        System.setProperty("blur.base.shard.port",baseShardPort+"");
        System.setProperty("blur.base.controller.port",baseControllerPort+"");
        System.setProperty("baseGuiShardPort",baseGuiShardPort+"");
        System.setProperty("baseGuiControllerPort",baseGuiControllerPort+"");
        System.setProperty("blur.gui.mode", base);
        LOG.info("System props:" + System.getProperties().toString());

        WebAppContext context = new WebAppContext();
        context.setWar(getJarFolder() + "../src/blur-gui/src/main/webapps/controller");// + base);
        context.setContextPath("/");
        context.setParentLoaderPriority(true);
        //servlets
        context.addServlet(new ServletHolder(new MetricsServlet(bm)), "/metrics");
        context.addServlet(new ServletHolder(new LogServlet(blurLogFile)), "/logs");
        
        LOG.info("WEB GUI coming up for resource: " + base);
        LOG.info("WEB GUI thinks its at: " + getJarFolder());
        LOG.info("WEB GUI log file being exposed: " + blurLogFile);
        
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
		    s = s.replace('/', File.separatorChar);
		    s = s.substring(0, s.indexOf(".jar")+4);
		    s = s.substring(s.lastIndexOf(':')+1);
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
