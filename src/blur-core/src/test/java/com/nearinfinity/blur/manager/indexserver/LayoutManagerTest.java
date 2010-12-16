package com.nearinfinity.blur.manager.indexserver;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import org.junit.Test;

public class LayoutManagerTest {
    
    @Test
    public void testLayoutManager() {
        TreeSet<String> nodes = new TreeSet<String>();
        nodes.add("n1");
        nodes.add("n2");
        nodes.add("n3");
        
        TreeSet<String> nodesOffline = new TreeSet<String>();
        nodesOffline.add("n2");
        
        TreeSet<String> shards = new TreeSet<String>();
        shards.add("s1");
        shards.add("s2");
        shards.add("s3");
        shards.add("s4");
        shards.add("s5");
        
        LayoutManager layoutManager1 = new LayoutManager().setNodes(nodes).setShards(shards).init();
        Map<String, String> layout1 = layoutManager1.getLayout();
        
        LayoutManager layoutManager2 = new LayoutManager().setNodes(nodes).setShards(shards).setNodesOffline(nodesOffline).init();
        Map<String, String> layout2 = layoutManager2.getLayout();
        
        assertEquals(shards, new TreeSet<String>(layout1.keySet()));
        assertEquals(nodes, new TreeSet<String>(layout1.values()));
        
        assertEquals(shards, new TreeSet<String>(layout2.keySet()));
        TreeSet<String> nodesOnline = new TreeSet<String>(nodes);
        nodesOnline.removeAll(nodesOffline);
        assertEquals(nodesOnline, new TreeSet<String>(layout2.values()));

    }
    
    @Test
    public void testLayoutManagerPerformance() {
        LayoutManager perfTest = new LayoutManager().setNodes(getTestNodes()).setShards(getTestShards()).setNodesOffline(getTestOfflineNodes()).init();
        int testSize = 100000;
        for (int i = 0; i < testSize; i++) {
            perfTest.getLayout();
        }
        long s = System.nanoTime();
        for (int i = 0; i < testSize; i++) {
            perfTest.getLayout();
        }
        long e = System.nanoTime();
        double ms = (e-s) / 1000000.0;
        System.out.println("Total    " + ms);
        System.out.println("Per Call " + ms / testSize);
        assertTrue(ms < 100);
    }
    
    private static Collection<String> getTestOfflineNodes() {
        return Arrays.asList("n13");
    }

    private static Collection<String> getTestShards() {
        Collection<String> shards = new HashSet<String>();
        for (int i = 0; i < 701; i++) {
            shards.add("s" + i);   
        }
        return shards;
    }

    private static Collection<String> getTestNodes() {
        Collection<String> nodes = new HashSet<String>();
        for (int i = 0; i < 32; i++) {
            nodes.add("n" + i);   
        }
        return nodes;
    }

}
