package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.manager.IndexManagerTest.rm;
import static com.nearinfinity.blur.thrift.ThriftUtil.newColumn;
import static com.nearinfinity.blur.thrift.ThriftUtil.newColumnFamily;
import static com.nearinfinity.blur.thrift.ThriftUtil.newRow;
import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.blur.manager.ComplexIndexManagerTest;
import com.nearinfinity.blur.manager.LocalHdfsMeleConfiguration;
import com.nearinfinity.blur.manager.util.MeleFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.mele.Mele;

public class BlurShardServerTest {
    
    private static final int PORT = 9123;
    private static TThreadPoolServer server;
    private static Mele mele;
    private static Thread thread;
    private static Client client;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        rm(new File("target/test-tmp"));
        MeleFactory.setup(new LocalHdfsMeleConfiguration());
        mele = MeleFactory.getInstance();
        mele.createDirectoryCluster(ComplexIndexManagerTest.TABLE_NAME);
        mele.createDirectory(ComplexIndexManagerTest.TABLE_NAME, ComplexIndexManagerTest.SHARD_NAME);
        
        TServerSocket serverTransport = new TServerSocket(PORT);
        Blur.Processor processor = new Blur.Processor(new BlurShardServer());
        Factory protFactory = new TBinaryProtocol.Factory(true, true);
        server = new TThreadPoolServer(processor, serverTransport, protFactory);
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        thread.setDaemon(true);
        thread.start();
        
        TTransport tr = new TSocket("localhost", PORT);
        TProtocol proto = new TBinaryProtocol(tr);
        client = new Blur.Client(proto);
        tr.open();
        
        
        
        List<String> tableList = client.tableList();
        if (tableList.contains(ComplexIndexManagerTest.TABLE_NAME)) {
            TableDescriptor describe = client.describe(ComplexIndexManagerTest.TABLE_NAME);
            if (describe.isEnabled) {
                client.disable(ComplexIndexManagerTest.TABLE_NAME);
            }
            client.drop(ComplexIndexManagerTest.TABLE_NAME);
        }
        
        TableDescriptor desc = new TableDescriptor();
        desc.analyzerDef = "";
        desc.partitionerClass = "com.nearinfinity.blur.manager.Partitioner";
        desc.addToShardNames(ComplexIndexManagerTest.SHARD_NAME);
        
        client.create(ComplexIndexManagerTest.TABLE_NAME, desc);
        client.enable(ComplexIndexManagerTest.TABLE_NAME);
    }
    
    @Test
    public void testAddData() throws BlurException, MissingShardException, TException {
        Row row = newRow("1000", 
                newColumnFamily("person", "1234", 
                        newColumn("private","true"),
                        newColumn("name", "aaron mccurry", "aaron patrick mccurry", "aaron p mccurry"),
                        newColumn("gender", "male"),
                        newColumn("dob","19781008","19781000")),
                newColumnFamily("address","5678",
                        newColumn("private","true"),
                        newColumn("street","155 johndoe","155 johndoe Court"),
                        newColumn("city","thecity")));
        
        client.replaceRow(ComplexIndexManagerTest.TABLE_NAME, row);
        Row fetchRow = client.fetchRow(ComplexIndexManagerTest.TABLE_NAME, "1000");
        assertEquals(row,fetchRow);
    }

}
