/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.lucene.store;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZookeeperLockFactory extends LockFactory {

    private final static Log LOG = LogFactory.getLog(ZookeeperLockFactory.class);
    private ZooKeeper zk;
    private String indexLockPath;

    public ZookeeperLockFactory(ZooKeeper zk, String indexLockPath) {
        this.indexLockPath = indexLockPath;
        this.zk = zk;
        mkNodesStr(zk, indexLockPath);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
        LOG.info("Clearing Lock [" + lockName + "]");
    }

    @Override
    public Lock makeLock(String lockName) {
        try {
            return new ZookeeperLock(zk, indexLockPath, lockName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ZookeeperLock extends Lock {

        private ZooKeeper zk;
        private String instanceIndexLockPath;

        public ZookeeperLock(ZooKeeper zk, String indexLockPath, String name) throws IOException {
            mkNodesStr(zk, indexLockPath);
            this.zk = zk;
            this.instanceIndexLockPath = getPath(indexLockPath, name);
        }

        @Override
        public boolean isLocked() throws IOException {
            try {
                Stat stat = zk.exists(instanceIndexLockPath, false);
                if (stat == null) {
                    return false;
                }
                return true;
            } catch (KeeperException e) {
                throw new IOException(e);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public boolean obtain() throws IOException {
            try {
                zk.create(instanceIndexLockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                return true;
            } catch (KeeperException e) {
                if (e.code() == Code.NODEEXISTS) {
                    return false;
                }
                throw new IOException(e);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void release() throws IOException {
            try {
                zk.delete(instanceIndexLockPath, -1);
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (KeeperException e) {
                throw new IOException(e);
            }
        }

    }
    
    public static void mkNodesStr(ZooKeeper zk, String path) {
        if (path == null) {
            return;
        }
        String[] split = path.split("/");
        for (int i = 0; i < split.length; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                if (!split[j].isEmpty()) {
                    builder.append('/');
                    builder.append(split[j]);
                }
            }
            String pathToCheck = builder.toString();
            if (pathToCheck.isEmpty()) {
                continue;
            }
            try {
                if (zk.exists(pathToCheck, false) == null) {
                    zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (NodeExistsException e) {
                // do nothing
            } catch (KeeperException e) {
                LOG.error("error", e);
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.error("error", e);
                throw new RuntimeException(e);
            }
        }
    }
    
    public static String getPath(String... parts) {
        if (parts == null || parts.length == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            builder.append('/');
            builder.append(parts[i]);
        }
        return builder.toString();
    }
}
