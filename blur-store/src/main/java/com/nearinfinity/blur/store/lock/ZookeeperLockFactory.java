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

package com.nearinfinity.blur.store.lock;

import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZookeeperLockFactory extends LockFactory {

    private final static Log LOG = LogFactory.getLog(ZookeeperLockFactory.class);
    private ZooKeeper zk;
    private String indexLockPath;
    private String holderName;

    public ZookeeperLockFactory(ZooKeeper zk, String indexLockPath, String holderName) {
        this.indexLockPath = indexLockPath;
        this.zk = zk;
        this.holderName = holderName;
        ZkUtils.mkNodesStr(zk, indexLockPath);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
        LOG.info("Clearing Lock [{0}]",lockName);
    }

    @Override
    public Lock makeLock(String lockName) {
        try {
            return new ZookeeperLock(zk, indexLockPath, lockName, holderName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ZookeeperLock extends Lock {

        private ZooKeeper zk;
        private String instanceIndexLockPath;
        private byte[] holderName;

        public ZookeeperLock(ZooKeeper zk, String indexLockPath, String name, String holderName) throws IOException {
            ZkUtils.mkNodesStr(zk, indexLockPath);
            this.zk = zk;
            this.holderName = holderName.getBytes();
            this.instanceIndexLockPath = ZkUtils.getPath(indexLockPath, name);
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
                zk.create(instanceIndexLockPath, holderName, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
}
