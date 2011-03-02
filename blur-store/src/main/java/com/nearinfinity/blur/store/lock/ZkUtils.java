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

package com.nearinfinity.blur.store.lock;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

/** @author Aaron McCurry (amccurry@nearinfinity.com) */
public class ZkUtils {

    private final static Log LOG = LogFactory.getLog(ZkUtils.class);

    public static final int ANY_VERSION = -1;

    public static void mkNodesStr(ZooKeeper zk, String path) {
        if (path == null) {
            return;
        }
        mkNodes(zk, path.split("/"));
    }

    public static void mkNodes(ZooKeeper zk, String... path) {
        if (path == null) {
            return;
        }
        for (int i = 0; i < path.length; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j <= i; j++) {
                if (!path[j].isEmpty()) {
                    builder.append('/');
                    builder.append(path[j]);
                }
            }
            String pathToCheck = removeDupSeps(builder.toString());
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

    private static String removeDupSeps(String path) {
        return path.replace("//", "/");
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

    public static boolean exists(ZooKeeper zk, String... path) {
        if (path == null || path.length == 0) {
            return true;
        }
        StringBuilder builder = new StringBuilder(path[0]);
        for (int i = 1; i < path.length; i++) {
            builder.append('/').append(path[i]);
        }
        try {
            return zk.exists(builder.toString(), false) != null;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void deleteAnyVersion(ZooKeeper zk, String path) {
        try {
            List<String> children = zk.getChildren(path, false);
            for (String c : children) {
                deleteAnyVersion(zk, path + "/" + c);
            }
            zk.delete(path, ANY_VERSION);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NONODE) {
                return;
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
