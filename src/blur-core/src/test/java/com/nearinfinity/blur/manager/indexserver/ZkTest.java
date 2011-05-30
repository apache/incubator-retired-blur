/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.manager.indexserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
        zkInMemory.registerCallableOnChange(new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                
            }
        }, "test","test");
    }
    
    public static class ZkInMemory extends DistributedManager {
        
        public List<String> pathes = new ArrayList<String>();
        public Map<String,byte[]> data = new HashMap<String, byte[]>();
        public List<String> callbacks = new ArrayList<String>();
        public List<Watcher> callbacksRunnable = new ArrayList<Watcher>();

        @Override
        public void close() {
            
        }

        @Override
        protected void createEphemeralPathInternal(String path) {
            if (pathes.contains(path)) {
                return;
            }
            pathes.add(path);
        }

        @Override
        protected void createPathInternal(String path) {
            if (pathes.contains(path)) {
                return;
            }
            pathes.add(path);
            int size = callbacks.size();
            for (int i = 0; i < size; i++) {
                String callPath = callbacks.get(i);
                if (callPath != null && path.startsWith(callPath)) {
                    if (countSlashes(path) == countSlashes(callPath) + 1) {
                        Watcher runnable = callbacksRunnable.get(i);
                        if (runnable != null) {
                            runnable.process(null);
                            callbacks.set(i, null);
                            callbacksRunnable.set(i, null);
                        }
                    }
                }
            }
        }

        private int countSlashes(String s) {
            int count = 0;
            char[] charArray = s.toCharArray();
            for (int i = 0; i < charArray.length; i++) {
                if (charArray[i] == '/') {
                    count++;
                }
            }
            return count;
        }

        @Override
        protected boolean existsInternal(String path) {
            return pathes.contains(path);
        }

        @Override
        protected List<String> listInternal(String path) {
            List<String> results = new ArrayList<String>();
            for (String p : pathes) {
                if (p.startsWith(path) && !p.equals(path)) {
                    String str = p.substring(path.length() + 1);
                    results.add(removeEverythingAfterSlash(str));
                }
            }
            return results;
        }

        private String removeEverythingAfterSlash(String str) {
            int indexOf = str.indexOf('/');
            if (indexOf < 0) {
                return str;
            }
            return str.substring(0,indexOf);
        }

        @Override
        protected void registerCallableOnChangeInternal(Watcher watcher, String path) {
            callbacks.add(path);
            callbacksRunnable.add(watcher);
        }

        @Override
        protected void removeEphemeralPathOnShutdownInternal(String path) {
            
        }

        @Override
        protected void fetchDataInternal(Value value, String path) {
            value.data = data.get(path);
        }

        @Override
        protected void lockInternal(String path) {
            
        }

        @Override
        protected boolean saveDataInternal(byte[] data, String path) {
            return false;
        }

        @Override
        protected void unlockInternal(String path) {
            
        }

        @Override
        protected void removePath(String path) {
            
        }
        
    }
}
