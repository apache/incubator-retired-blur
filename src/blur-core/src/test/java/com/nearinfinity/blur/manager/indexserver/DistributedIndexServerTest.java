package com.nearinfinity.blur.manager.indexserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Test;

public class DistributedIndexServerTest {
    
    private static final String TEST = "test";
    private static final List<String> NODE_LIST = Arrays.asList("node1","node2","node3","node4");
    private static final List<String> SHARD_LIST = Arrays.asList("a","b","c","d","e","f","g","h","i");
    
    @After
    public void tearDown() {
        
    }

    @Test
    public void testDistributedIndexServer() throws IOException {
        List<String> shardBeingServed = new ArrayList<String>();
        for (String node : NODE_LIST) {
            DistributedIndexServer indexServer = new MockDistributedIndexServer(NODE_LIST,SHARD_LIST).
                setNodeName(node).
                init();
            shardBeingServed.addAll(indexServer.getIndexReaders(TEST).keySet());
            indexServer.close();
        }
        Collections.sort(shardBeingServed);
        assertEquals(SHARD_LIST,shardBeingServed);
    }
    
    @Test
    public void testReaderCloserDaemon() throws IOException, InterruptedException {
        List<String> nodes = new ArrayList<String>(NODE_LIST);
        final List<String> toBeClosed = new ArrayList<String>();
        toBeClosed.add("d");
        toBeClosed.add("h");
        DistributedIndexServer indexServer = new MockDistributedIndexServer(nodes, SHARD_LIST) {
            @Override
            protected void beforeClose(String shard, IndexReader indexReader) {
                assertTrue(toBeClosed.contains(shard));
                toBeClosed.remove(shard);
            }
        }.
            setNodeName("node2").
            setDelay(5000).
            init();
        assertEquals(new TreeSet<String>(Arrays.asList("d","h")), new TreeSet<String>(indexServer.getIndexReaders(TEST).keySet()));
        nodes.remove(3);
        assertEquals(new TreeSet<String>(Arrays.asList("c","f","i")), new TreeSet<String>(indexServer.getIndexReaders(TEST).keySet()));
        Thread.sleep(10000);
        assertTrue(toBeClosed.isEmpty());
        indexServer.close();
    }
    
    public static class MockDistributedIndexServer extends DistributedIndexServer {
        
        private List<String> shards;
        private List<String> nodes;

        public MockDistributedIndexServer(List<String> nodes, List<String> shards) {
            this.shards = shards;
            this.nodes = nodes;
        }

        @Override
        protected IndexReader openShard(String table, String shard) {
            return getEmptyIndexReader();
        }
        
        private IndexReader getEmptyIndexReader() {
            try {
                return IndexReader.open(getEmptyDir());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private Directory getEmptyDir() throws CorruptIndexException, LockObtainFailedException, IOException {
            RAMDirectory directory = new RAMDirectory();
            new IndexWriter(directory, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED).close();
            return directory;
        }

        @Override
        public List<String> getShardList(String table) {
            return shards;
        }

        @Override
        public List<String> getShardServerList() {
            return nodes;
        }
        
        @Override
        public void close() {
            
        }

        @Override
        public Analyzer getAnalyzer(String table) {
            throw new RuntimeException("not implement");
        }

        @Override
        public List<String> getControllerServerList() {
            throw new RuntimeException("not implement");
        }
        
        @Override
        public Similarity getSimilarity(String table) {
            throw new RuntimeException("not implement");
        }

        @Override
        public List<String> getTableList() {
            return Arrays.asList(TEST);
        }

        @Override
        public TABLE_STATUS getTableStatus(String table) {
            throw new RuntimeException("not implement");
        }

        @Override
        protected void beforeClose(String shard, IndexReader indexReader) {
            throw new RuntimeException("not implement");            
        }

    }

}
