package com.nearinfinity.blur.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.manager.indexserver.ClusterStatus;
import com.nearinfinity.blur.thrift.client.BlurClient;
import com.nearinfinity.blur.thrift.client.BlurClientEmbedded;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;

public class BlurControllerServerTest {
    
    private static final String TABLE = "test";
    private Map<String,Iface> shardServers = new HashMap<String, Iface>();
    private BlurControllerServer server;
    
    @Before
    public void setup() {
        addShardServer("node1");
        addShardServer("node2");
        addShardServer("node3");
        server = new BlurControllerServer();
        server.setClient(getClient());
        server.setClusterStatus(getClusterStatus());
        server.open();
    }
    
    private ClusterStatus getClusterStatus() {
        return new ClusterStatus() {

            @Override
            public List<String> controllerServerList() {
                throw new RuntimeException("no impl");
            }

            @Override
            public List<String> getOnlineShardServers() {
                return shardServerList();
            }

            @Override
            public List<String> shardServerList() {
                List<String> nodes = new ArrayList<String>(shardServers.keySet());
                Collections.sort(nodes);
                return nodes;
            }
            
        };
    }

    @After
    public void tearDown() {
        server.close();
    }

    @Test
    public void testSearch() throws BlurException, TException {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.maxQueryTime = TimeUnit.SECONDS.toMillis(5);
        searchQuery.minimumNumberOfHits = Long.MAX_VALUE;
        Hits hits = server.search(TABLE, searchQuery);
        assertNotNull(hits);
    }
    
    @Test
    public void testRecordFrequency() throws BlurException, TException {
        long recordFrequency = server.recordFrequency(TABLE, "cf", "cn", "value");
        assertEquals(3,recordFrequency);
    }

    private BlurClient getClient() {
        BlurClientEmbedded blurClientEmbedded = new BlurClientEmbedded();
        for (String node : shardServers.keySet()) {
            blurClientEmbedded.putNode(node, shardServers.get(node));
        }
        return blurClientEmbedded;
    }

    
    private Iface getShardServer(final String node) {
        return new Iface() {
            
            @Override
            public List<String> terms(String arg0, String arg1, String arg2, String arg3, short arg4) throws BlurException,
                    TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public List<String> tableList() throws BlurException, TException {
                List<String> table = new ArrayList<String>();
                table.add(TABLE);
                return table;
            }
            
            @Override
            public List<String> shardServerList() throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
                Map<String,String> layout = new HashMap<String, String>();
                layout.put(node, node);
                return layout;
            }
            
            @Override
            public Hits search(String arg0, SearchQuery arg1) throws BlurException, TException {
                Hits hits = new Hits();
                hits.putToShardInfo(node, 0);
                return hits;
            }
            
            @Override
            public Schema schema(String arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public long recordFrequency(String arg0, String arg1, String arg2, String arg3) throws BlurException, TException {
                return 1l;
            }
            
            @Override
            public FetchResult fetchRow(String arg0, Selector arg1) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public TableDescriptor describe(String arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public List<String> controllerServerList() throws BlurException, TException {
                throw new RuntimeException("no impl");
            }
            
            @Override
            public void cancelSearch(long arg0) throws BlurException, TException {
                throw new RuntimeException("no impl");                
            }
        };
    }
    
    private void addShardServer(String node) {
        shardServers.put(node, getShardServer(node));
    }
}
