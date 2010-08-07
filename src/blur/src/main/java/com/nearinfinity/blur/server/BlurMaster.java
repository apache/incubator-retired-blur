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
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;

import com.nearinfinity.blur.BlurClient;
import com.nearinfinity.blur.data.DataStorage;
import com.nearinfinity.blur.data.DataStorage.DataResponse;
import com.nearinfinity.blur.manager.SearchExecutor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.Merger;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;

public class BlurMaster extends BlurServer implements SearchExecutor {
	
	public static void main(String[] args) throws Exception {
		int port = Integer.parseInt(args[0]);
		Server server = new Server(port);
		server.setHandler(new BlurMaster());
		server.start();
		server.join();
	}

	private static final String DATA = "data";
	private BlurConfiguration configuration = new BlurConfiguration();
	private List<BlurClient> clients = new ArrayList<BlurClient>();
	private DataStorage dataStorage;
	
	public BlurMaster() {
		dataStorage = configuration.getNewInstance(BLUR_DATA_STORAGE_STORE_CLASS, DataStorage.class);
		searchExecutor = this;
		String[] blurNodes = new String[]{"localhost:8081","localhost:8082"};
		createBlurClients(blurNodes);
	}

	private void createBlurClients(String[] blurNodes) {
		for (String blurNode : blurNodes) {
			clients.add(new BlurClient(blurNode));
		}
	}

	@Override
	public Set<String> getTables() {
		return new HashSet<String>();
	}

	@Override
	public BlurHits search(ExecutorService executor, final String table, final String query, final String filter, final long start, final int fetchCount) {
		try {
			return ForkJoin.execute(executor, clients, new ParallelCall<BlurClient,BlurHits>() {
				@Override
				public BlurHits call(BlurClient client) throws Exception {
					return client.search(table, query, filter, start, fetchCount);
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
	public long searchFast(ExecutorService executor, final String table, final String query, final String filter, final long minimum) {
		try {
			return ForkJoin.execute(executor, clients, new ParallelCall<BlurClient,Long>() {
				@Override
				public Long call(BlurClient client) throws Exception {
					return client.searchFast(table, query, filter, minimum);
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
		
	}

	@Override
	protected void handleOther(String target, Request baseRequest, HttpServletRequest request, final HttpServletResponse response) throws IOException {
		String[] split = target.split("/");
		if (split.length > 3) {
			if (DATA.equals(split[2])) {
				DataResponse simpleResponse = new DataResponse();
				dataStorage.fetch(split[3], simpleResponse);
				response.setContentType(simpleResponse.getMimeType());
				IOUtils.copy(simpleResponse.getInputStream(), response.getOutputStream());
				return;
			}
		}
		super.handleOther(target, baseRequest, request, response);
	}

}
