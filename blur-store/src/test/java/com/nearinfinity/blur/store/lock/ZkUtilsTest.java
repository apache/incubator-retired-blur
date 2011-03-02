package com.nearinfinity.blur.store.lock;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ZkUtilsTest {

    private static final int ANY_VERSION = -1;

    private ZooKeeper zooKeeper;

    @Before
    public void setUp() throws Exception {
        try {
            zooKeeper = new ZooKeeper("127.0.0.1", 10000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                }
            });
        }
        catch (IOException e) {
            fail("ZooKeeper must be running locally");
        }
    }

    @After
    public void tearDown() throws Exception {
        delete("/test/foo/bar");
        delete("/test/foo");
        delete("/test");
        zooKeeper.close();
    }

    @Test
    public void testMkNodesStrWhenNoNodesExist() throws Exception {
        assertDoesNotExist("/test/foo/bar");
        ZkUtils.mkNodesStr(zooKeeper, "/test/foo/bar");
        assertExists("/test/foo/bar");
    }


    @Test
    public void testMkNodesStrWhenSomeNodesExist() throws Exception {
        create("/test");
        create("/test/foo");
        assertThat(zooKeeper.exists("/test/foo/bar", false), is(nullValue()));
        ZkUtils.mkNodesStr(zooKeeper, "/test/foo/bar");
        assertExists("/test/foo/bar");
    }

    @Test
    public void testMkNodesWhenNoNodesExist() throws Exception {
        assertDoesNotExist("/test/foo/bar");
        ZkUtils.mkNodes(zooKeeper, "test", "foo", "bar");
        assertExists("/test/foo/bar");
    }

    @Test
    public void testMkNodesWhenSomeNodesExist() throws Exception {
        create("/test");
        create("/test/foo");
        assertThat(zooKeeper.exists("/test/foo/bar", false), is(nullValue()));
        ZkUtils.mkNodes(zooKeeper, "/test", "foo", "bar");
        assertExists("/test/foo/bar");
    }

    private void create(String path) throws KeeperException, InterruptedException {
        zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void delete(String path) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, false) != null) {
            zooKeeper.delete(path, ANY_VERSION);
        }
    }

    private void assertDoesNotExist(String path) throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(path, false), is(nullValue()));
    }

    private void assertExists(String path) throws InterruptedException, KeeperException {
        assertThat(zooKeeper.exists(path, false), is(notNullValue()));
    }


}
