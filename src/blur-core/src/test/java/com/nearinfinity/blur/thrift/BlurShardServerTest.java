package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.junit.After;
import org.junit.Before;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;

public class BlurShardServerTest {
    
    private BlurShardServer blurShardServer;

    @Before
    public void setUp() {
        IndexServer indexServer = getIndexServer();
        IndexManager indexManager = getIndexManager().setIndexServer(indexServer);
        indexManager.init();
        blurShardServer = (BlurShardServer) new BlurShardServer().
            setIndexManager(indexManager).
            setIndexServer(indexServer);
    }
    
    @After
    public void tearDown() throws InterruptedException {
        blurShardServer.close();
    }
    
    private IndexServer getIndexServer() {
        return new IndexServer() {

            @Override
            public void close() {
                
            }

            @Override
            public Analyzer getAnalyzer(String table) {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getControllerServerList() {
                throw new RuntimeException("not impl");
            }

            @Override
            public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getShardList(String table) {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getShardServerList() {
                throw new RuntimeException("not impl");
            }

            @Override
            public Similarity getSimilarity(String table) {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getTableList() {
                throw new RuntimeException("not impl");
            }

            @Override
            public TABLE_STATUS getTableStatus(String table) {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getOfflineShardServers() {
                throw new RuntimeException("not impl");
            }

            @Override
            public List<String> getOnlineShardServers() {
                throw new RuntimeException("no impl");
            }

            @Override
            public String getNodeName() {
                throw new RuntimeException("no impl");
            }
            
        };
    }

    private IndexManager getIndexManager() {
        return new IndexManager() {
            
        };
    }

}
