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

import static com.nearinfinity.blur.utils.BlurUtil.quietClose;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TFramedTransport.Factory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown;
import com.nearinfinity.blur.manager.indexserver.HdfsIndexServer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.BlurServerShutDown.BlurShutdown;
import com.nearinfinity.blur.manager.indexserver.ManagedDistributedIndexServer.NODE_TYPE;
import com.nearinfinity.blur.store.cache.LocalFileCache;
import com.nearinfinity.blur.store.cache.LocalFileCacheCheck;
import com.nearinfinity.blur.store.replication.ReplicationDaemon;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Processor;

public class ThriftBlurShardServer {
    
    public static final String CRAZY = "CRAZY";
    public static final String BLUR_BIND_ADDRESS = "blur.bind.address";

    private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);
    
    private String nodeName;
    private Iface iface;
    private TThreadPoolServer server;
    private boolean closed;
    
    public static void main(String[] args) throws TTransportException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Unknown error in thread [{0}]",e,t);
            }
        });
        
        String nodeName = args[0];
        String zkConnectionStr = args[1];
        String hdfsPath = args[2];
        List<File> localFileCaches = new ArrayList<File>();
        for (String cachePath : args[3].split(",")) {
            localFileCaches.add(new File(cachePath));
        }
        boolean crazyMode = false;
        if (args.length == 5 && args[4].equals(CRAZY)) {
            crazyMode = true;
        }
        
        final ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        
        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);
        
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path blurBasePath = new Path(hdfsPath);
        
        final LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setPotentialFiles(localFileCaches.toArray(new File[]{}));
        localFileCache.init();
        
        LockFactory lockFactory = new NoLockFactory();
        
        final ReplicationDaemon replicationDaemon = new ReplicationDaemon();
        replicationDaemon.setLocalFileCache(localFileCache);
        replicationDaemon.init();
        
        final HdfsIndexServer indexServer = new HdfsIndexServer();
        indexServer.setType(NODE_TYPE.SHARD);
        indexServer.setLocalFileCache(localFileCache);
        indexServer.setLockFactory(lockFactory);
        indexServer.setFileSystem(fileSystem);
        indexServer.setBlurBasePath(blurBasePath);
        indexServer.setNodeName(nodeName);
        indexServer.setDistributedManager(dzk);
        indexServer.setReplicationDaemon(replicationDaemon);
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
        if (crazyMode) {
            System.err.println("Crazy mode!!!!!");
            server.setIface(crazyMode(shardServer));            
        } else {
            server.setIface(shardServer);
        }
        
        // This will shutdown the server when the correct path is set in zk
        new BlurServerShutDown().register(new BlurShutdown() {
            @Override
            
            public void shutdown() {
                quietClose(replicationDaemon,server,shardServer,indexManager,indexServer,localFileCache);
                System.exit(0);
            }
        }, zooKeeper);
        
        server.start();
    }

    public synchronized void close() {
        if (!closed) {
            closed = true;
            server.stop();
        }
    }

    private static LocalFileCacheCheck getLocalFileCacheCheck(final HdfsIndexServer indexServer) {
        return new LocalFileCacheCheck() {
            @Override
            public boolean isSafeForRemoval(String table, String shard, String name) throws IOException {
                Map<String, IndexReader> indexReaders = indexServer.getIndexReaders(table);
                if (indexReaders.containsKey(shard)) {
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

    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(ThriftBlurShardServer.parse(System.getProperty(BLUR_BIND_ADDRESS, nodeName)));
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new BlurSearch.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
        LOG.info("Starting server [{0}]",nodeName);
        server.serve();
    }

    public static InetSocketAddress parse(String nodeName) {
        return new InetSocketAddress(getHost(nodeName), getPort(nodeName));
    }

    private static String getHost(String nodeName) {
        return nodeName.substring(0,nodeName.indexOf(':'));
    }

    private static int getPort(String nodeName) {
        return Integer.parseInt(nodeName.substring(nodeName.indexOf(':') + 1));
    }

    public Iface getIface() {
        return iface;
    }

    public void setIface(Iface iface) {
        this.iface = iface;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

}
