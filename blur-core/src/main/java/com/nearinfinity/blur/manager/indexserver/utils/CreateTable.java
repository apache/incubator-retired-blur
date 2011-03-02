package com.nearinfinity.blur.manager.indexserver.utils;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;

public class CreateTable implements ZookeeperPathConstants {

    public static void main(String[] args) throws IOException {
        String zkConnectionStr = args[0];
        String table = args[1];
        BlurAnalyzer analyzer = BlurAnalyzer.create(new File(args[2]));

        ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        ZookeeperDistributedManager dm = new ZookeeperDistributedManager();
        dm.setZooKeeper(zooKeeper);
        if (dm.exists(BLUR_TABLES, table)) {
            System.err.println("Table [" + table + "] already exists.");
            System.exit(1);
        }
        dm.createPath(BLUR_TABLES, table);
        dm.saveData(analyzer.toString().getBytes(), BLUR_TABLES, table);
    }

}
