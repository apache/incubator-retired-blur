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

package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurConstants.BLUR_LOCAL_CACHE_PATHES;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_TABLE_PATH;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static com.nearinfinity.blur.utils.BlurConstants.CRAZY;
import static com.nearinfinity.blur.utils.BlurUtil.quietClose;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.SimpleUncaughtExceptionHandler;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown;
import com.nearinfinity.blur.manager.indexserver.HdfsIndexServer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.manager.indexserver.ManagedDistributedIndexServer.NODE_TYPE;
import com.nearinfinity.blur.manager.writer.BlurIndex;
import com.nearinfinity.blur.store.cache.HdfsUtil;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.cache.LocalFileCacheCheck;
import com.nearinfinity.blur.store.replication.ReplicationDaemon;
import com.nearinfinity.blur.store.replication.ReplicationStrategy;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class ThriftBlurShardServer extends ThriftServer {
    
    private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);
    
    public static void main(String[] args) throws TTransportException, IOException {
        LOG.info("Setting up Shard Server");
        Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
        
        BlurConfiguration configuration = new BlurConfiguration();

        String nodeName = getNodeName(configuration,BLUR_SHARD_HOSTNAME);
        String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION),BLUR_ZOOKEEPER_CONNECTION);
        String tablePath = isEmpty(configuration.get(BLUR_TABLE_PATH),BLUR_TABLE_PATH);
        String localCacheDirs = isEmpty(configuration.get(BLUR_LOCAL_CACHE_PATHES),BLUR_LOCAL_CACHE_PATHES);
        
        List<File> localFileCaches = new ArrayList<File>();
        for (String cachePath : localCacheDirs.split(",")) {
            localFileCaches.add(new File(cachePath));
        }
        boolean crazyMode = false;
        if (args.length == 1 && args[0].equals(CRAZY)) {
            crazyMode = true;
        }

        final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);

        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);

        Path blurBasePath = new Path(tablePath);
        FileSystem fileSystem = FileSystem.get(blurBasePath.toUri(), new Configuration());

        final LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(localFileCaches.toArray(new File[localFileCaches.size()]));
        localFileCache.init();

        LockFactory lockFactory = new NoLockFactory();

        final ReplicationDaemon replicationDaemon = new ReplicationDaemon();
        replicationDaemon.setLocalFileCache(localFileCache);
        replicationDaemon.init();

        ReplicationStrategy replicationStrategy = new ReplicationStrategy() {
            @Override
            public boolean replicateLocally(String table, String name) {
                if (name.endsWith(".fdt")) {
                    return false;
                }
                return false;
            }
        };

        final HdfsIndexServer indexServer = new HdfsIndexServer();
        indexServer.setType(NODE_TYPE.SHARD);
        indexServer.setLocalFileCache(localFileCache);
        indexServer.setLockFactory(lockFactory);
        indexServer.setFileSystem(fileSystem);
        indexServer.setBlurBasePath(blurBasePath);
        indexServer.setNodeName(nodeName);
        indexServer.setDistributedManager(dzk);
        indexServer.setReplicationDaemon(replicationDaemon);
        indexServer.setReplicationStrategy(replicationStrategy);
        indexServer.init();

        localFileCache.setLocalFileCacheCheck(getLocalFileCacheCheck(indexServer));

        final IndexManager indexManager = new IndexManager();
        indexManager.setIndexServer(indexServer);
        indexManager.init();

        final BlurShardServer shardServer = new BlurShardServer();
        shardServer.setIndexServer(indexServer);
        shardServer.setIndexManager(indexManager);

        final ThriftBlurShardServer server = new ThriftBlurShardServer();
        server.setNodeName(nodeName);
        server.setAddressPropertyName(BLUR_SHARD_BIND_ADDRESS);
        server.setPortPropertyName(BLUR_SHARD_BIND_PORT);
        if (crazyMode) {
            System.err.println("Crazy mode!!!!!");
            server.setIface(crazyMode(shardServer));
        }
        else {
            server.setIface(shardServer);
        }
        server.setConfiguration(configuration);

        // This will shutdown the server when the correct path is set in zk
        new BlurServerShutDown().register(new BlurShutdown() {
            @Override
            public void shutdown() {
                quietClose(replicationDaemon, server, shardServer, indexManager, indexServer, localFileCache);
                System.exit(0);
            }
        }, zooKeeper);
        
        server.start();
    }

    private static LocalFileCacheCheck getLocalFileCacheCheck(final HdfsIndexServer indexServer) {
        return new LocalFileCacheCheck() {
            @Override
            public boolean isSafeForRemoval(String dirName, String name) throws IOException {
                String table = HdfsUtil.getTable(dirName);
                String shard = HdfsUtil.getShard(dirName);
                Map<String, BlurIndex> blurIndexes = indexServer.getIndexes(table);
                if (blurIndexes.containsKey(shard)) {
                    return false;
                }
                return true;
            }
        };
    }

    public static Iface crazyMode(final Iface iface) {
        return (Iface) Proxy.newProxyInstance(Iface.class.getClassLoader(), new Class[] { Iface.class },
            new InvocationHandler() {
                private Random random = new Random();
                private long strikeTime = System.currentTimeMillis() + 100;
                @Override
                public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    long now = System.currentTimeMillis();
                    if (strikeTime < now) {
                        strikeTime = now + random.nextInt(2000);
                        System.err.println("Crazy Monkey Strikes!!! Next strike [" + strikeTime + "]");
                        throw new RuntimeException("Crazy Monkey Strikes!!!");
                    }
                    return method.invoke(iface, args);
                }
            });
    }

}
