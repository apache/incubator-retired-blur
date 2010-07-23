package com.nearinfinity.blur.engine;

import java.io.IOException;

import com.nearinfinity.blur.BlurSearch;
import com.nearinfinity.blur.SearchResult;
import com.nearinfinity.blur.messaging.BlurRpcClient;
import com.nearinfinity.blur.messaging.MessageUtil;

public class BlurClient implements BlurSearch {
	
	private BlurRpcClient client;
	
	public static void main(String[] args) throws IOException {
		BlurRpcClient blurClient = new BlurRpcClient("localhost",3000);
		BlurClient blurQueryEngine = new BlurClient(blurClient);
		while (true) {
			long s = System.currentTimeMillis();
			SearchResult searchResult = blurQueryEngine.search("test:test", "", 0, 10);
			long e = System.currentTimeMillis();
			System.out.println(searchResult.count + " in " + (e-s) + " ms");
		}
	}

	public BlurClient(BlurRpcClient client) {
		this.client = client;
	}

	@Override
	public SearchResult searchFast(String query, String filter) {
		return searchFast(query, filter, Long.MAX_VALUE);
	}

	@Override
	public SearchResult searchFast(String query, String filter, long minimum) {
		try {
			byte[] searchFast = client.send(MessageUtil.getSearchFastMessage(query,filter,minimum));
			return MessageUtil.getSearchResult(searchFast);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public SearchResult search(String query, String filter, long starting, int fetch) {
		try {
			byte[] search = client.send(MessageUtil.getSearchMessage(query,filter,starting,fetch));
			return MessageUtil.getSearchResult(search);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
