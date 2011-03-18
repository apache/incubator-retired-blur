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

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.analysis.BlurAnalyzer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.zookeeper.ZkUtils;

import static com.nearinfinity.blur.manager.indexserver.ZookeeperPathConstants.BLUR_TABLES;

public class CreateTable {

    public static void main(String[] args) throws IOException {
        String zkConnectionStr = args[0];
        String table = args[1];
        BlurAnalyzer analyzer = BlurAnalyzer.create(new File(args[2]));

        ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
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
