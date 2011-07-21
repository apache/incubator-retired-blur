package com.nearinfinity.blur.demo;

import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_ADDRESS;
import static com.nearinfinity.blur.utils.BlurConstants.BLUR_SHARD_BIND_PORT;

import java.io.File;
import java.io.IOException;

import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.indexserver.LocalIndexServer;
import com.nearinfinity.blur.thrift.BlurShardServer;
import com.nearinfinity.blur.thrift.ThriftBlurShardServer;

public class DemoServer {

    public static void main(String[] args) throws IOException, TTransportException {
        if (args.length != 1) {
            System.err.println("DemoServer <path to demo tables>");
            System.exit(1);
        }
        BlurConfiguration configuration = new BlurConfiguration();
        IndexServer indexServer = new LocalIndexServer(new File(args[0]));
        
        IndexManager indexManager = new IndexManager();
        indexManager.setIndexServer(indexServer);
        indexManager.init();
        
        final BlurShardServer shardServer = new BlurShardServer();
        shardServer.setIndexServer(indexServer);
        shardServer.setIndexManager(indexManager);

        final ThriftBlurShardServer server = new ThriftBlurShardServer();
        server.setNodeName("demo-server");
        server.setAddressPropertyName(BLUR_SHARD_BIND_ADDRESS);
        server.setPortPropertyName(BLUR_SHARD_BIND_PORT);
        server.setIface(shardServer);
        server.setConfiguration(configuration);
        server.start();

    }

}
