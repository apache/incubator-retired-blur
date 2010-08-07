package using;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.nearinfinity.blur.zookeeper.ZookeeperDirectoryManagerStore;

public class AddingTablesToZookeeper {
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		String table = "tablename";
//		BlurConfiguration configuration = new BlurConfiguration();
		ZookeeperDirectoryManagerStore store = new ZookeeperDirectoryManagerStore();
		store.addDirectoryURIToServe(table, "shard1", new URI("zk://localhost/blur/refs/"+table+"/shard1?file:///Users/amccurry/testIndex"));
		store.addDirectoryURIToServe(table, "shard2", new URI("zk://localhost/blur/refs/"+table+"/shard2?file:///Users/amccurry/testIndex"));
//		
//		System.out.println(store.getDirectoryURIToServe(table, "shard1"));
//		System.out.println(store.getDirectoryURIToServe(table, "shard2"));
//		
//		System.out.println(store.getShardIdsToServe(configuration.getNodeUuid(), 1));
//		System.out.println(store.getShardIdsToServe(configuration.getNodeUuid(), 1));
//		System.out.println(store.getShardIdsToServe(configuration.getNodeUuid(), 1));
//		
//		System.out.println(store.getShardIds(table));
//		System.out.println(store.getTables());
//		
//		store.removeDirectoryURIToServe(table, "shard1");
//		
//		System.out.println(store.getTables());
//		
//		System.out.println(store.getShardIds(table));
//		System.out.println(store.getTables());
//		
//		store.removeTable(table);
//		
//		System.out.println(store.getTables());
	}

}
