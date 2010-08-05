package using;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.nearinfinity.blur.manager.dao.DirectoryManagerStore;

public class LocalDirectoryManagerStore implements DirectoryManagerStore {

	@Override
	public Map<String, Set<String>> getShardIdsToServe() {
		Map<String, Set<String>> shardIds = new TreeMap<String, Set<String>>();
		shardIds.put("test", new TreeSet<String>(Arrays.asList("test")));
		return shardIds;
	}
	
	@Override
	public URI getURIForShardId(String table, String shardId) {
		try {
			return new URI("zk://localhost/blur/test/testIndex?file:///Users/amccurry/testIndex");
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}

}
