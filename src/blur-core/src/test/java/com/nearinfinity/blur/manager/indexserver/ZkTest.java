package com.nearinfinity.blur.manager.indexserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class ZkTest {

    private ZkInMemory zkInMemory;

    @Before
    public void setUp() {
        zkInMemory = new ZkInMemory();
    }
    
    @Test
    public void testExists() {
        assertFalse(zkInMemory.exists("test","one","the"));
        zkInMemory.createPath("test","one","the");
        assertTrue(zkInMemory.exists("test","one","the"));
        
        assertFalse(zkInMemory.exists("test","two","the"));
        zkInMemory.createEphemeralPath("test","two","the");
        assertTrue(zkInMemory.exists("test","two","the"));
    }
    
    @Test
    public void testList() {
        assertTrue(zkInMemory.list("test","one","the").isEmpty());
        zkInMemory.createPath("test","one","the");
        assertEquals(1,zkInMemory.list("test","one").size());
    }
    
    @Test
    public void testCallable() {
        zkInMemory.registerCallableOnChange(new Runnable() {
            @Override
            public void run() {
                
            }
        }, "test","test");
    }
    
    class ZkInMemory extends DistributedManager {
        
        private List<String> pathes = new ArrayList<String>();

        @Override
        public void close() {
            
        }

        @Override
        protected void createEphemeralPathInternal(String path) {
            pathes.add(path);
        }

        @Override
        protected void createPathInternal(String path) {
            pathes.add(path);
        }

        @Override
        protected boolean existsInternal(String path) {
            return pathes.contains(path);
        }

        @Override
        protected List<String> listInternal(String path) {
            List<String> results = new ArrayList<String>();
            for (String p : pathes) {
                if (p.startsWith(path)) {
                    results.add(p.substring(path.length() + 1));
                }
            }
            return results;
        }

        @Override
        protected void registerCallableOnChangeInternal(Runnable runnable, String path) {
            
        }

        @Override
        protected void removeEphemeralPathOnShutdownInternal(String path) {
            
        }
        
    }
}
