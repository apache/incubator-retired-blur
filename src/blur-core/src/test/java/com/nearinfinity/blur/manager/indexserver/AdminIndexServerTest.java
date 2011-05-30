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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;

import com.nearinfinity.blur.concurrent.ExecutorsDynamicConfig;
import com.nearinfinity.blur.concurrent.SimpleExecutorsDynamicConfig;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.indexserver.ZkTest.ZkInMemory;
import com.nearinfinity.blur.manager.writer.BlurIndex;

public class AdminIndexServerTest {

    private static final String TESTTABLE = "testtable";
    private AdminIndexServer adminIndexServer;
    private ZkInMemory dm;
    private ExecutorsDynamicConfig dynamicConfig;

    @Before
    public void setup() {
        dynamicConfig = new SimpleExecutorsDynamicConfig(10);
        dm = new ZkInMemory();
        adminIndexServer = newAdminIndexServer();
        adminIndexServer.setNodeName("me");
        adminIndexServer.setDistributedManager(dm);
        adminIndexServer.setDynamicConfig(dynamicConfig);
        adminIndexServer.init();
    }

//    @Test
    public void testAdminIndexServerTableList() {
        assertTrue(adminIndexServer.getTableList().isEmpty());
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(),TESTTABLE);
        assertEquals(newList(TESTTABLE),adminIndexServer.getTableList());
    }
    
//    @Test
    public void testAdminIndexServerTableListTimer() throws InterruptedException {
        assertTrue(adminIndexServer.getTableList().isEmpty());
        dm.pathes.add(ZookeeperPathConstants.getBlurTablesPath() + "/" + TESTTABLE);
        assertTrue(adminIndexServer.getTableList().isEmpty());
        Thread.sleep(TimeUnit.SECONDS.toMillis(12));// wait for the 10 second timer to pick up the change
        assertEquals(newList(TESTTABLE), adminIndexServer.getTableList());
    }
    
//    @Test
    public void testAdminIndexServerTableStatus() {
        assertEquals(TABLE_STATUS.DISABLED,adminIndexServer.getTableStatus(TESTTABLE));
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(),TESTTABLE);
        assertEquals(TABLE_STATUS.DISABLED,adminIndexServer.getTableStatus(TESTTABLE));
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(),TESTTABLE,ZookeeperPathConstants.getBlurTablesEnabled());
        assertEquals(TABLE_STATUS.ENABLED,adminIndexServer.getTableStatus(TESTTABLE));
    }
    
//    @Test
    public void testAdminIndexServerTableAnalyzer() throws InterruptedException {
        assertEquals(AdminIndexServer.BLANK_ANALYZER,adminIndexServer.getAnalyzer(TESTTABLE));
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(),TESTTABLE);
        assertEquals(AdminIndexServer.BLANK_ANALYZER,adminIndexServer.getAnalyzer(TESTTABLE));
        dm.data.put(ZookeeperPathConstants.getBlurTablesPath() + "/" + TESTTABLE, "{\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}".getBytes());
        Thread.sleep(TimeUnit.SECONDS.toMillis(12));// wait for the 10 second timer to pick up the change
        assertFalse(AdminIndexServer.BLANK_ANALYZER.equals(adminIndexServer.getAnalyzer(TESTTABLE)));
    }

    private List<String> newList(String... strs) {
        return new ArrayList<String>(Arrays.asList(strs));
    }

    private AdminIndexServer newAdminIndexServer() {
        return new AdminIndexServer() {

            @Override
            public void close() {
                throw new RuntimeException();
            }

            @Override
            public List<String> getControllerServerList() {
                throw new RuntimeException();
            }

            @Override
            public Map<String, BlurIndex> getIndexes(String table) throws IOException {
                System.out.println("Do nothing.");
                return new HashMap<String, BlurIndex>();
            }

            @Override
            public List<String> getOfflineShardServers() {
                throw new RuntimeException();
            }

            @Override
            public List<String> getOnlineShardServers() {
                throw new RuntimeException();
            }

            @Override
            public List<String> getShardList(String table) {
                throw new RuntimeException();
            }

            @Override
            public List<String> getShardServerList() {
                throw new RuntimeException();
            }

            @Override
            protected void tableStatusChanged(String table) {
                throw new RuntimeException();
            }
        };
    }
}
