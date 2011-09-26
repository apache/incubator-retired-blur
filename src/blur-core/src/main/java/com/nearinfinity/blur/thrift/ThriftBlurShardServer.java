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

import static com.nearinfinity.blur.utils.BlurConstants.BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_LOCAL_CACHE_PATHS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_MAX_CLAUSE_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_CACHE_MAX_TIMETOLIVE;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_HOSTNAME;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_OPENER_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_CONNECTION;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE;
import static com.nearinfinity.blur.utils.BlurConstants.CRAZY;
import static com.nearinfinity.blur.utils.BlurUtil.quietClose;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.concurrent.SimpleUncaughtExceptionHandler;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.BlurQueryChecker;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.clusterstatus.ZookeeperClusterStatus;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown;
import com.nearinfinity.blur.manager.indexserver.HdfsIndexServer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.manager.indexserver.ManagedDistributedIndexServer.NODE_TYPE;
import com.nearinfinity.blur.manager.writer.BlurIndexCommiter;
import com.nearinfinity.blur.manager.writer.BlurIndexRefresher;
import com.nearinfinity.blur.store.blockcache.BlockCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectoryCache;
import com.nearinfinity.blur.store.blockcache.BlockDirectory;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.zookeeper.ZkUtils;

public class ThriftBlurShardServer extends ThriftServer {

    private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);

    public static void main(String[] args) throws TTransportException, IOException, KeeperException, InterruptedException, BlurException {
        LOG.info("Setting up Shard Server");
        Thread.setDefaultUncaughtExceptionHandler(new SimpleUncaughtExceptionHandler());
        
        //setup block cache
        //134,217,728 is the bank size, therefore there are 8,192 block 
        //in a bank when using a block of 16,384
        int numberOfBlocksPerBank = 8192;
        int blockSize = BlockDirectory.BLOCK_SIZE;
        int numberOfBanks = getNumberOfBanks(0.5f,numberOfBlocksPerBank,blockSize);
        BlockCache blockCache = new BlockCache(numberOfBanks,numberOfBlocksPerBank,blockSize);
        BlockDirectoryCache cache = new BlockDirectoryCache(blockCache);

        BlurConfiguration configuration = new BlurConfiguration();

        String nodeNameHostName = getNodeName(configuration, BLUR_SHARD_HOSTNAME);
        String nodeName = nodeNameHostName + ":" + configuration.get(BLUR_SHARD_BIND_PORT);
        String zkConnectionStr = isEmpty(configuration.get(BLUR_ZOOKEEPER_CONNECTION), BLUR_ZOOKEEPER_CONNECTION);
        String localCacheDirs = isEmpty(configuration.get(BLUR_LOCAL_CACHE_PATHS), BLUR_LOCAL_CACHE_PATHS);
        
        BlurQueryChecker queryChecker = new BlurQueryChecker(configuration);

        List<File> localFileCaches = new ArrayList<File>();
        for (String cachePath : localCacheDirs.split(",")) {
            localFileCaches.add(new File(cachePath));
        }
        boolean crazyMode = false;
        if (args.length == 1 && args[0].equals(CRAZY)) {
            crazyMode = true;
        }

        final ZooKeeper zooKeeper = ZkUtils.newZooKeeper(zkConnectionStr);
        ZookeeperSystemTime.checkSystemTime(zooKeeper, configuration.getLong(BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE, 3000));

        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);
        
        final ZookeeperClusterStatus clusterStatus = new ZookeeperClusterStatus(zooKeeper);

        final BlurIndexRefresher refresher = new BlurIndexRefresher();
        refresher.init();
        
        final BlurIndexCommiter commiter = new BlurIndexCommiter();
        commiter.init();

        final HdfsIndexServer indexServer = new HdfsIndexServer();
        indexServer.setCommiter(commiter);
        indexServer.setType(NODE_TYPE.SHARD);
        indexServer.setZookeeper(zooKeeper);
        indexServer.setNodeName(nodeName);
        indexServer.setDistributedManager(dzk);
        indexServer.setRefresher(refresher);
        indexServer.setShardOpenerThreadCount(configuration.getInt(BLUR_SHARD_OPENER_THREAD_COUNT, 16));
        indexServer.setClusterStatus(clusterStatus);
        indexServer.setCache(cache);
        indexServer.init();

        final IndexManager indexManager = new IndexManager();
        indexManager.setIndexServer(indexServer);
        indexManager.setMaxClauseCount(configuration.getInt(BLUR_MAX_CLAUSE_COUNT, 1024));
        indexManager.setThreadCount(configuration.getInt(BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT, 32));
        indexManager.init();

        final BlurShardServer shardServer = new BlurShardServer();
        shardServer.setIndexServer(indexServer);
        shardServer.setIndexManager(indexManager);
        shardServer.setDistributedManager(dzk);
        shardServer.setClusterStatus(clusterStatus);
        shardServer.setMaxQueryCacheElements(configuration.getInt(BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS,128));
        shardServer.setMaxTimeToLive(configuration.getLong(BLUR_SHARD_CACHE_MAX_TIMETOLIVE,TimeUnit.MINUTES.toMillis(1)));
        shardServer.setQueryChecker(queryChecker);
        shardServer.init();

        int threadCount = configuration.getInt(BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT, 32);
        
        final ThriftBlurShardServer server = new ThriftBlurShardServer();
        server.setNodeName(nodeName);
        server.setAddressPropertyName(BLUR_SHARD_BIND_ADDRESS);
        server.setPortPropertyName(BLUR_SHARD_BIND_PORT);
        server.setThreadCount(threadCount);
        if (crazyMode) {
            System.err.println("Crazy mode!!!!!");
            server.setIface(crazyMode(shardServer));
        } else {
            server.setIface(shardServer);
        }
        server.setConfiguration(configuration);

        // This will shutdown the server when the correct path is set in zk
        new BlurServerShutDown().register(new BlurShutdown() {
            @Override
            public void shutdown() {
                quietClose(commiter, refresher, server, shardServer, indexManager, indexServer);
                System.exit(0);
            }
        }, zooKeeper);
        server.start();
    }

    public static int getNumberOfBanks(float heapPercentage, int numberOfBlocksPerBank, int blockSize) {
      long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
      long targetBytes = (long) (max * heapPercentage);
      int slabSize = numberOfBlocksPerBank * blockSize;
      int slabs = (int) (targetBytes / slabSize);
      if (slabs == 0) {
        throw new RuntimeException("Minimum heap size is 512m!");
      }
      LOG.info("Block cache parameters, target heap usage [" + heapPercentage + "] slab size of [" + slabSize + 
          "] will allocate [" + slabs + "] slabs and use ~[" + ((long) slabs * (long) slabSize) + "] bytes");
      return slabs;
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
