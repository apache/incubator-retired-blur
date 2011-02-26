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

import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.indexserver.ZkTest.ZkInMemory;

public class AdminIndexServerTest implements ZookeeperPathContants {

    private static final String TESTTABLE = "testtable";
    private AdminIndexServer adminIndexServer;
    private ZkInMemory dm;

    @Before
    public void setup() {
        dm = new ZkInMemory();
        adminIndexServer = newAdminIndexServer();
        adminIndexServer.setNodeName("me");
        adminIndexServer.setDistributedManager(dm);
        adminIndexServer.init();
    }

    @Test
    public void testAdminIndexServerTableList() {
        assertTrue(adminIndexServer.getTableList().isEmpty());
        dm.createPath(BLUR_TABLES,TESTTABLE);
        assertEquals(newList(TESTTABLE),adminIndexServer.getTableList());
    }
    
    @Test
    public void testAdminIndexServerTableListTimer() throws InterruptedException {
        assertTrue(adminIndexServer.getTableList().isEmpty());
        dm.pathes.add(BLUR_TABLES + "/" + TESTTABLE);
        assertTrue(adminIndexServer.getTableList().isEmpty());
        Thread.sleep(TimeUnit.SECONDS.toMillis(12));// wait for the 10 second timer to pick up the change
        assertEquals(newList(TESTTABLE), adminIndexServer.getTableList());
    }
    
    @Test
    public void testAdminIndexServerTableStatus() {
        assertEquals(TABLE_STATUS.DISABLED,adminIndexServer.getTableStatus(TESTTABLE));
        dm.createPath(BLUR_TABLES,TESTTABLE);
        assertEquals(TABLE_STATUS.DISABLED,adminIndexServer.getTableStatus(TESTTABLE));
        dm.createPath(BLUR_TABLES,TESTTABLE,BLUR_TABLES_ENABLED);
        assertEquals(TABLE_STATUS.ENABLED,adminIndexServer.getTableStatus(TESTTABLE));
    }
    
    @Test
    public void testAdminIndexServerTableAnalyzer() throws InterruptedException {
        assertEquals(AdminIndexServer.BLANK_ANALYZER,adminIndexServer.getAnalyzer(TESTTABLE));
        dm.createPath(BLUR_TABLES,TESTTABLE);
        assertEquals(AdminIndexServer.BLANK_ANALYZER,adminIndexServer.getAnalyzer(TESTTABLE));
        dm.data.put(BLUR_TABLES + "/" + TESTTABLE, "{\"default\":\"org.apache.lucene.analysis.standard.StandardAnalyzer\"}".getBytes());
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
            public Map<String, IndexReader> getIndexReaders(String table) throws IOException {
                System.out.println("Do nothing.");
                return new HashMap<String, IndexReader>();
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

        };
    }
}
