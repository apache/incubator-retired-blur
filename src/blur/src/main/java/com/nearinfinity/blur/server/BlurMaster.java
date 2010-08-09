package com.nearinfinity.blur.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;

import com.nearinfinity.blur.data.DataStorage;
import com.nearinfinity.blur.data.DataStorage.DataResponse;
import com.nearinfinity.blur.manager.SearchExecutor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.blur.zookeeper.ZooKeeperFactory;

public class BlurMaster extends BlurServer implements SearchExecutor,BlurConstants, Watcher {
	
	public static void main(String[] args) throws Exception {
		BlurMaster master = new BlurMaster();
		master.startServer();
	}

	private static final String DATA = "data";
	private BlurConfiguration configuration = new BlurConfiguration();
	private List<BlurClient> clients = new ArrayList<BlurClient>();
	private DataStorage dataStorage;
	private ZooKeeper zk = ZooKeeperFactory.getZooKeeper();
	
	public BlurMaster() throws IOException {
		super();
		port = configuration.getInt(BLUR_MASTER_PORT, 40000);
		init();
	}

	public BlurMaster(int port) throws IOException {
		super();
		this.port = port;
		init();
	}
	
	private void init() {
		dataStorage = configuration.getNewInstance(BLUR_DATA_STORAGE_STORE_CLASS, DataStorage.class);
		searchExecutor = this;
		createBlurClients();
	}

	private synchronized void createBlurClients() {
		try {
			List<BlurClient> newClients = new ArrayList<BlurClient>();
			List<String> children = zk.getChildren(blurNodePath, this);
			for (String child : children) {
				String path = blurNodePath + "/" + child;
				Stat stat = zk.exists(path, false);
				if (stat != null) {
					byte[] bs = zk.getData(path, false, stat);
					if (isNode(bs)) {
						newClients.add(new BlurClient(child));
					}
				}
			}
			clients = newClients;
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isNode(byte[] bs) {
		NODE_TYPE node = NODE_TYPE.valueOf(new String(bs));
		if (node.equals(NODE_TYPE.NODE)) {
			return true;
		}
		return false;
	}

	@Override
	public Set<String> getTables() {
		return new HashSet<String>();
	}

	@Override
	public BlurHits search(ExecutorService executor, final String table, final String query, final String acl, final long start, final int fetchCount) {
		try {
			return ForkJoin.execute(executor, clients, new ParallelCall<BlurClient,BlurHits>() {
				@Override
				public BlurHits call(BlurClient client) throws Exception {
					return client.search(table, query, acl, start, fetchCount);
				}
			}).merge(new Merger<BlurHits>() {
				@Override
				public BlurHits merge(List<Future<BlurHits>> futures) throws Exception {
					BlurHits blurHits = null;
					for (Future<BlurHits> future : futures) {
						if (blurHits == null) {
							blurHits = future.get();
						} else {
							blurHits.merge(future.get());
						}
					}
					blurHits.reduceHitsTo(fetchCount);
					return blurHits;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long searchFast(ExecutorService executor, final String table, final String query, final String acl, final long minimum) {
		try {
			return ForkJoin.execute(executor, clients, new ParallelCall<BlurClient,Long>() {
				@Override
				public Long call(BlurClient client) throws Exception {
					return client.searchFast(table, query, acl, minimum);
				}
			}).merge(new Merger<Long>() {
				@Override
				public Long merge(List<Future<Long>> futures) throws Exception {
					long total = 0;
					for (Future<Long> future : futures) {
						total += future.get();
					}
					return total;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void update() {
		createBlurClients();
	}

	@Override
	protected void handleOther(String target, Request baseRequest, HttpServletRequest request, final HttpServletResponse response) throws IOException {
		String[] split = target.split("/");
		if (split.length > 3) {
			if (DATA.equals(split[2])) {
				DataResponse simpleResponse = new DataResponse();
				dataStorage.fetch(split[1], split[3], simpleResponse);
				response.setContentType(simpleResponse.getMimeType());
				IOUtils.copy(simpleResponse.getInputStream(), response.getOutputStream());
				return;
			}
		}
		super.handleOther(target, baseRequest, request, response);
	}

	@Override
	public void startServer() throws Exception {
		Server server = new Server(port);
		server.setHandler(this);
		server.start();
		registerNode();
		server.join();
	}

	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.MASTER;
	}

	@Override
	public void process(WatchedEvent event) {
		createBlurClients();
	}
}
