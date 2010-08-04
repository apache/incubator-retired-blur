package com.nearinfinity.blur.server;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.eclipse.jetty.server.Server;

import com.nearinfinity.blur.manager.SearchExecutor;

public class BlurMaster extends BlurServer implements SearchExecutor {

	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);
		server.setHandler(new BlurMaster());
		server.start();
		server.join();
	}
	
	public BlurMaster() {
		searchExecutor = this;
	}

	@Override
	public Set<String> getTables() {
		return null;
	}

	@Override
	public BlurHits search(ExecutorService executor, String table, String query, String filter, long start, int fetchCount) {
		return null;
	}

	@Override
	public long searchFast(ExecutorService executor, String table, String query, String filter, long minimum) {
		return 0;
	}

	@Override
	public void update() {
		
	}

}
