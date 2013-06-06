package org.apache.blur.agent.collectors.hdfs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.blur.agent.Agent;
import org.apache.blur.agent.connections.hdfs.interfaces.HdfsDatabaseInterface;
import org.apache.blur.agent.exceptions.HdfsThreadException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HdfsCollector implements Runnable {
	private static final Log log = LogFactory.getLog(HdfsCollector.class);

	private final URI defaultUri;
	private final String hdfsName;
	private final String user;
	private final HdfsDatabaseInterface databaseConnection;
	private final boolean collectHdfs;

	public HdfsCollector(final String hdfsName, final String defaultUri, final String thriftUri, final String user,
			final List<String> activeCollectors, final HdfsDatabaseInterface databaseConnection) throws HdfsThreadException {
		try {
			this.defaultUri = new URI(defaultUri);
			this.hdfsName = hdfsName;
			this.user = user;
			this.databaseConnection = databaseConnection;
			this.collectHdfs = activeCollectors.contains("hdfs");

			initializeHdfs(hdfsName, thriftUri);

		} catch (URISyntaxException e) {
			log.error(e.getMessage(), e);
			throw new HdfsThreadException();
		} catch (Exception e) {
			log.error("An unkown error occured while creating the collector.", e);
			throw new HdfsThreadException();
		}
	}

	@Override
	public void run() {
		while (true) {
			if (this.collectHdfs) {
				new Thread(new HdfsStatsCollector(this.hdfsName, defaultUri, this.user, this.databaseConnection), "Hdfs Collector - "
						+ this.hdfsName).start();
			}

			try {
				Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private void initializeHdfs(String name, String thriftUri) throws URISyntaxException {
		URI parsedThriftUri = new URI(thriftUri);
		this.databaseConnection.setHdfsInfo(name, parsedThriftUri.getHost(), parsedThriftUri.getPort());
	}
}