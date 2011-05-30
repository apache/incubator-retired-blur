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

package com.nearinfinity.blur.manager.indexserver.utils;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.indexserver.DistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class EnableTable {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String zkConnectionStr = args[0];
        String table = args[1];

        ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
        ZookeeperDistributedManager dm = new ZookeeperDistributedManager();
        dm.setZooKeeper(zooKeeper);
        enableTable(dm, table);
    }
    
    public static void enableTable(DistributedManager dm, String table) throws IOException {
        if (!dm.exists(ZookeeperPathConstants.getBlurTablesPath(), table)) {
            throw new IOException("Table [" + table + "] does not exist.");
        }
        if (dm.exists(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesEnabled())) {
            throw new IOException("Table [" + table + "] already enabled.");
        }
        dm.createPath(ZookeeperPathConstants.getBlurTablesPath(), table, ZookeeperPathConstants.getBlurTablesEnabled());
    }

}
