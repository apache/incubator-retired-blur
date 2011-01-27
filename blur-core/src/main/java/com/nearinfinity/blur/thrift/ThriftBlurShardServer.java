package com.nearinfinity.blur.thrift;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.indexserver.HdfsIndexServer;
import com.nearinfinity.blur.manager.indexserver.ZookeeperDistributedManager;
import com.nearinfinity.blur.manager.indexserver.ManagedDistributedIndexServer.NODE_TYPE;
import com.nearinfinity.blur.store.HdfsExistenceCheck;
import com.nearinfinity.blur.store.LocalFileCache;
import com.nearinfinity.blur.thrift.generated.BlurSearch;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Processor;

public class ThriftBlurShardServer {
    
    public static final String BLUR_BIND_ADDRESS = "blur.bind.address";

    private static final Log LOG = LogFactory.getLog(ThriftBlurShardServer.class);
    
    private String nodeName;
    private Iface iface;
    
    public static void main(String[] args) throws TTransportException, IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Unknown error in thread [" + t +
                		"]",e);
            }
        });
        
        String nodeName = args[0];
        String zkConnectionStr = args[1];
        String hdfsPath = args[2];
        List<File> localFileCaches = new ArrayList<File>();
        for (String cachePath : args[3].split(",")) {
            localFileCaches.add(new File(cachePath));
        }
        
        ZooKeeper zooKeeper = new ZooKeeper(zkConnectionStr, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
        
        ZookeeperDistributedManager dzk = new ZookeeperDistributedManager();
        dzk.setZooKeeper(zooKeeper);
        
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path blurBasePath = new Path(hdfsPath);
        
        HdfsExistenceCheck existenceCheck = new HdfsExistenceCheck(fileSystem, blurBasePath);

        LocalFileCache localFileCache = new LocalFileCache();
        localFileCache.setExistenceCheck(existenceCheck);
        localFileCache.setPotentialFiles(localFileCaches.toArray(new File[]{}));
        localFileCache.open();
        
        LockFactory lockFactory = new NoLockFactory();
        
        HdfsIndexServer indexServer = new HdfsIndexServer();
        indexServer.setType(NODE_TYPE.SHARD);
        indexServer.setLocalFileCache(localFileCache);
        indexServer.setLockFactory(lockFactory);
        indexServer.setFileSystem(fileSystem);
        indexServer.setBlurBasePath(blurBasePath);
        indexServer.setNodeName(nodeName);
        indexServer.setDistributedManager(dzk);
        indexServer.init();
        
        IndexManager indexManager = new IndexManager();
        indexManager.setIndexServer(indexServer);
        indexManager.init();
        
        BlurShardServer shardServer = new BlurShardServer();
        shardServer.setIndexServer(indexServer);
        shardServer.setIndexManager(indexManager);
        
        ThriftBlurShardServer server = new ThriftBlurShardServer();
        server.setNodeName(nodeName);
        server.setIface(shardServer);
        server.start();
    }

    public void start() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(ThriftBlurShardServer.parse(System.getProperty(BLUR_BIND_ADDRESS, nodeName)));
        Factory transportFactory = new TFramedTransport.Factory();
        Processor processor = new BlurSearch.Processor(iface);
        TBinaryProtocol.Factory protFactory = new TBinaryProtocol.Factory(true, true);
        TThreadPoolServer server = new TThreadPoolServer(processor, serverTransport, transportFactory, protFactory);
        LOG.info("Starting server [" + nodeName + "]");
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
