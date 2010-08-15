package using;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.nearinfinity.blur.manager.DirectoryManagerStore;

public class LocalDirectoryManagerStore implements DirectoryManagerStore {

	@Override
	public Map<String, Set<String>> getShardIdsToServe(String nodeId, int maxToServePerCall) {
		Map<String, Set<String>> shardIds = new TreeMap<String, Set<String>>();
		shardIds.put("test", new TreeSet<String>(Arrays.asList("test")));
		return shardIds;
	}
	
	@Override
	public URI getDirectoryURIToServe(String table, String shardId) {
		try {
			return new URI("zk://localhost/blur/test/testIndex?file:///Users/amccurry/testIndex");
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void addDirectoryURIToServe(String table, String shardId, URI dirUri) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<String> getShardIds(String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getTables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeDirectoryURIToServe(String table, String shardId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeTable(String table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isThisNodeServing(String table, String shardId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean obtainLock(String table, String shardId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void releaseLock(String table, String shardId) {
		// TODO Auto-generated method stub
		
	}

}
