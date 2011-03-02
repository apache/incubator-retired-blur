package com.nearinfinity.blur.manager.indexserver.utils;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;

public class EnableTable implements ZookeeperPathConstants {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String zkConnectionStr = args[0];
        String table = args[1];

        ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        ZookeeperDistributedManager dm = new ZookeeperDistributedManager();
        dm.setZooKeeper(zooKeeper);
        if (!dm.exists(BLUR_TABLES, table)) {
            System.err.println("Table [" + table + "] does not exist.");
            System.exit(1);
        }
        if (dm.exists(BLUR_TABLES, table, BLUR_TABLES_ENABLED)) {
            System.err.println("Table [" + table + "] already enabled.");
            System.exit(1);
        }
        dm.createPath(BLUR_TABLES, table, BLUR_TABLES_ENABLED);
    }

}
