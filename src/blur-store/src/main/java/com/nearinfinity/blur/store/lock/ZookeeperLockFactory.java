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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.zookeeper.ZkUtils;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZookeeperLockFactory extends LockFactory {

    private final static Log LOG = LogFactory.getLog(ZookeeperLockFactory.class);
    private ZooKeeper _zk;
    private String _lockPath;
    
    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("localhost", 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                
            }
            
        });
        ZookeeperLock zookeeperLock1 = new ZookeeperLock(zk, "/test/locks", "myname1");
        ZookeeperLock zookeeperLock2 = new ZookeeperLock(zk, "/test/locks", "myname2");
        System.out.println(zookeeperLock1.isLocked());
        System.out.println(zookeeperLock1.obtain());
        System.out.println(zookeeperLock1.isLocked());
        
        System.out.println(zookeeperLock2.isLocked());
        System.out.println(zookeeperLock2.obtain());
        System.out.println(zookeeperLock2.isLocked());
        
        ZookeeperLock zookeeperLock3 = new ZookeeperLock(zk, "/test/locks", "myname2");
        while (!zookeeperLock3.obtain()) {
            Thread.sleep(5000);
            System.out.println("try again");
        }
    }

    public ZookeeperLockFactory(ZooKeeper zk, String sharedLockPath, String name) {
        _lockPath = sharedLockPath + "/" + name;
        _zk = zk;
        ZkUtils.mkNodesStr(_zk, _lockPath);
    }

    @Override
    public void clearLock(String lockName) throws IOException {
        LOG.info("Clearing Lock [{0}]",lockName);
    }

    @Override
    public Lock makeLock(String lockName) {
        try {
            return new ZookeeperLock(_zk, _lockPath, lockName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ZookeeperLock extends Lock {

        private ZooKeeper _zk;
        private String _instanceIndexLockPath;
        private Stat _stat;

        public ZookeeperLock(ZooKeeper zk, String lockPath, String lockName) throws IOException {
            ZkUtils.mkNodesStr(zk, lockPath);
            _zk = zk;
            _instanceIndexLockPath = ZkUtils.getPath(lockPath, lockName);
        }

        @Override
        public boolean isLocked() throws IOException {
            try {
                Stat stat = _zk.exists(_instanceIndexLockPath, false);
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
                _zk.create(_instanceIndexLockPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                _stat = _zk.exists(_instanceIndexLockPath, false);
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
                _zk.delete(_instanceIndexLockPath, _stat.getVersion());
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (KeeperException e) {
                throw new IOException(e);
            }
        }

    }
}
