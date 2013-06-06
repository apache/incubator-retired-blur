package org.apache.blur.agent.collectors.zookeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.blur.agent.Agent;
import org.apache.blur.agent.connections.zookeeper.interfaces.ZookeeperDatabaseInterface;
import org.apache.blur.agent.notifications.Notifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;


public class ZookeeperCollector implements Runnable {
	private static final Log log = LogFactory.getLog(ZookeeperCollector.class);

	private ZooKeeper zookeeper;
	private boolean connected;

	private final String url;
	private final String name;
	private final int id;
	private final ZookeeperDatabaseInterface database;

	public ZookeeperCollector(String url, String name, String blurConnection, ZookeeperDatabaseInterface database) {
		this.url = url;
		this.name = name;
		this.database = database;
		this.id = database.insertOrUpdateZookeeper(name, url, blurConnection);
	}

	@Override
	public void run() {
		while (true) {
			try {
				if (!this.connected) {
					this.zookeeper = new ZooKeeper(this.url, 3000, new Watcher() {
						@Override
						public void process(WatchedEvent event) {
							KeeperState state = event.getState();
							if (state == KeeperState.Disconnected || state == KeeperState.Expired) {
								log.warn("Zookeeper [" + name + "] disconnected event.");
								database.setZookeeperOffline(id);
								Notifier.getNotifier().notifyZookeeperOffline(name);
								connected = false;
							} else if (state == KeeperState.SyncConnected) {
								log.info("Zookeeper [" + name + "] session established.");
								connected = true;
							}
						}
					});
				}
			} catch (IOException e) {
				log.error("A zookeeper [" + this.name + "] connection could not be created, waiting 30 seconds.");
				// Sleep the thread for 30secs to give the Zookeeper a chance to become
				// available.
				try {
					Thread.sleep(30000);
					continue;
				} catch (InterruptedException ex) {
					log.info("Exiting Zookeeper [" + this.name + "] instance");
					return;
				}
			}

			if (this.connected) {
				new Thread(new ControllerCollector(this.id, this.zookeeper, this.database), "Controller Collector - " + this.name).start();
				new Thread(new ClusterCollector(this.id, this.zookeeper, this.database), "Cluster Collector - " + this.name).start();
			}

			testEnsembleHealth();

			try {
				Thread.sleep(Agent.COLLECTOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				log.info("Exiting Zookeeper [" + this.name + "] instance");
				return;
			}
		}
	}

	private void testEnsembleHealth() {
		String[] connections = this.url.split(",");
		List<String> onlineZookeepers = new ArrayList<String>();
		for (String connection : connections) {
			try {
				URI parsedConnection = new URI("my://" + connection);
				String host = parsedConnection.getHost();
				int port = parsedConnection.getPort() >= 0 ? parsedConnection.getPort() : 2181;
				byte[] reqBytes = new byte[4];
				ByteBuffer req = ByteBuffer.wrap(reqBytes);
				req.putInt(ByteBuffer.wrap("ruok".getBytes()).getInt());
				Socket socket = new Socket();
				socket.setSoLinger(false, 10);
				socket.setSoTimeout(20000);
				parsedConnection.getPort();
				socket.connect(new InetSocketAddress(host, port));

				InputStream response = socket.getInputStream();
				OutputStream question = socket.getOutputStream();

				question.write(reqBytes);

				byte[] resBytes = new byte[4];

				response.read(resBytes);
				String status = new String(resBytes);
				if (status.equals("imok")) {
					onlineZookeepers.add(connection);
				}
				socket.close();
				response.close();
				question.close();
			} catch (Exception e) {
				log.error("A connection to " + connection + " could not be made.", e);
			}
		}
		try {
			if (connections.length == onlineZookeepers.size()){
				this.database.setZookeeperOnline(id);
			} else if (connections.length < onlineZookeepers.size() * 2) {
				this.database.setZookeeperWarning(this.id);
			} else if (this.connected){
				this.database.setZookeeperFailure(this.id);
			} else {
				this.database.setZookeeperOffline(this.id);
			}
			this.database.setOnlineEnsembleNodes(new ObjectMapper().writeValueAsString(onlineZookeepers), this.id);
		} catch (Exception e) {
			log.error("The online ensemble nodes array could not be created, writing that they are all offline!");
			this.database.setOnlineEnsembleNodes("{}", this.id);
		}
	}
}
