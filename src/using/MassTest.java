package using;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.nearinfinity.blur.BlurHit;
import com.nearinfinity.blur.SearchResult;
import com.nearinfinity.blur.messaging.BlurRpcServer;
import com.nearinfinity.blur.messaging.MasterController;
import com.nearinfinity.blur.messaging.MessageUtil;
import com.nearinfinity.blur.messaging.MasterController.MessageJoiner;
import com.nearinfinity.blur.search.SearchMessageHandler;

public class MassTest {

	public static void main(String[] args) throws Exception {
		
		int port = 3001;
		String hostname = "localhost/";
		List<String> hosts = new ArrayList<String>();
		List<BlurRpcServer> servers = new ArrayList<BlurRpcServer>();
		for (int i = 0; i < 30; i++) {
			servers.add(startServer(port+i));
			hosts.add(hostname + (port+i));
		}
		
		MessageJoiner joiner = new MessageJoiner() {
			@Override
			public byte[] join(Collection<byte[]> responses) {
				long totalHits = 0;
				List<BlurHit> blurHits = new ArrayList<BlurHit>();
				List<byte[]> shardNames = new ArrayList<byte[]>();
				for (byte[] response : responses) {
					SearchResult searchResult;
					try {
						searchResult = MessageUtil.getSearchResult(response);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
					totalHits += searchResult.count;
					blurHits.addAll(searchResult.hits);
					shardNames.addAll(searchResult.respondingShards);
				}
				try {
					return MessageUtil.createSearchResults(totalHits, blurHits, shardNames);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
		
		new MasterController(3000, hosts, joiner).start();
		
//		Thread.sleep(10000);
//		
//		System.out.println("Stoping server...");
//		
//		servers.get(0).stop();

	}

	private static BlurRpcServer startServer(int port) throws Exception {
		BlurRpcServer blurServer = new BlurRpcServer(port, new SearchMessageHandler(new String[]{}));
		blurServer.start();
		return blurServer;
	}
	

}
