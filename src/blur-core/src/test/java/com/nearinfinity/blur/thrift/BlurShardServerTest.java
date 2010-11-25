package com.nearinfinity.blur.thrift;

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.thrift.generated.BlurAdmin.Client;
import com.nearinfinity.mele.Mele;

public class BlurShardServerTest {
    
    private static final String SHARD_NAME = "shard";
    private static final String TABLE_NAME = "blur-shard-test";
    private static final int PORT = 9123;
    private static TThreadPoolServer server;
    private static Mele mele;
    private static Thread thread;
    private static Client client;
    private static BlurShardServer blurServer;
    private static ZooKeeper zooKeeper;

//    @BeforeClass
//    public static void setUpOnce() throws Exception {
//        String pathname = "target/test-tmp-blur-shard";
//        rm(new File(pathname));
//        LocalHdfsMeleConfiguration configuration = new LocalHdfsMeleConfiguration(pathname);
//        zooKeeper = new ZooKeeper(configuration.getZooKeeperConnectionString(), 
//                configuration.getZooKeeperSessionTimeout(), new NoOpWatcher());
//        mele = new MeleBase(new NoRepMeleDirectoryFactory(), configuration, zooKeeper);
//        mele.createDirectoryCluster(TABLE_NAME);
//        mele.createDirectory(TABLE_NAME, SHARD_NAME);
//        
//        TServerSocket serverTransport = new TServerSocket(PORT);
//        ZkMetaData zkMetaData = new ZkMetaData(mele, configuration, zooKeeper);
//        blurServer = new BlurShardServer(zkMetaData, new BlurConfiguration());
//        BlurAdmin.Processor processor = new BlurAdmin.Processor(blurServer);
//        Factory protFactory = new TBinaryProtocol.Factory(true, true);
//        server = new TThreadPoolServer(processor, serverTransport, protFactory);
//        thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                server.serve();
//            }
//        });
//        thread.setDaemon(true);
//        thread.start();
//        
//        TTransport tr = new TSocket("localhost", PORT);
//        TProtocol proto = new TBinaryProtocol(tr);
//        client = new BlurAdmin.Client(proto);
//        tr.open();
//        
//        List<String> tableList = client.tableList();
//        if (tableList.contains(TABLE_NAME)) {
//            TableDescriptor describe = client.describe(TABLE_NAME);
//            if (describe.isEnabled) {
//                client.disable(TABLE_NAME);
//            }
//            client.drop(TABLE_NAME);
//        }
//        
//        TableDescriptor desc = new TableDescriptor();
//        desc.analyzerDef = "";
//        desc.partitionerClass = "com.nearinfinity.blur.manager.Partitioner";
//        desc.addToShardNames(SHARD_NAME);
//        
//        client.create(TABLE_NAME, desc);
//        client.enable(TABLE_NAME);
//        Thread.sleep(2000);//wait for server to come online and serve shards
//    }
//    
//    @AfterClass
//    public static void oneTimeTearDown() throws InterruptedException {
//        blurServer.close();
//        zooKeeper.close();
//    }
//    
//    @Test
//    public void testAddData() throws BlurException, MissingShardException, TException, EventStoppedExecutionException {
//        Row row = newRow("1000", 
//                newColumnFamily("person", "1234", 
//                        newColumn("private","true"),
//                        newColumn("name", "aaron mccurry", "aaron patrick mccurry", "aaron p mccurry"),
//                        newColumn("gender", "male"),
//                        newColumn("dob","19781008","19781000")),
//                newColumnFamily("address","5678",
//                        newColumn("private","true"),
//                        newColumn("street","155 johndoe","155 johndoe Court"),
//                        newColumn("city","thecity")));
//        
//        client.replaceRow(TABLE_NAME, row);
//        FetchResult fetchRow = client.fetchRow(TABLE_NAME, newSelector("1000"));
//        assertTrue(fetchRow.exists);
//        assertEquals(row,fetchRow.row);
//    }

}
