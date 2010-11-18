package com.nearinfinity.blur.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.nearinfinity.blur.thrift.BlurAdminServer.NODE_TYPE;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;
import com.nearinfinity.mele.MeleBase;
import com.nearinfinity.mele.MeleConfiguration;
import com.nearinfinity.mele.util.ZkUtils;

public class ZkMetaData implements MetaData, BlurConstants, Watcher {

    private static final Log LOG = LogFactory.getLog(ZkMetaData.class);

    private ZooKeeper zk;
    private String blurPath;
    private List<String> shardServerHosts = new ArrayList<String>();
    private List<String> controllerServerHosts = new ArrayList<String>();
    private String blurNodePath;
    private MeleConfiguration configuration;
    private Mele mele;
    
    public ZkMetaData(Mele mele, MeleConfiguration configuration, ZooKeeper zk) {
        this.blurPath = configuration.getProperty(BLUR_ZOOKEEPER_PATH,BLUR_ZOOKEEPER_PATH_DEFAULT);
        this.blurNodePath = blurPath + "/" + NODES;
        this.mele = mele;
        this.configuration = configuration;
        this.zk = zk;
        process(null);
    }

    @Override
    public List<String> tableList() throws BlurException {
        try {
            String path = ZkUtils.getPath(blurPath, BLUR_TABLES_NODE);
            List<String> children = zk.getChildren(path, false);
            return new ArrayList<String>(children);
        } catch (KeeperException e) {
            if (e.code().equals(Code.NONODE)) {
                return new ArrayList<String>();
            }
            throw new BlurException(e.getMessage());
        } catch (InterruptedException e) {
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException {
        try {
            TableDescriptor descriptor = describe(table);
            Map<String, String> result = new TreeMap<String, String>();
            List<String> shardNames = descriptor.shardNames;
            for (String shardName : shardNames) {
                String lockPath = MeleBase.getLockPath(configuration, table, shardName);
                byte[] data = zk.getData(lockPath + "/" + "write.lock", false, null);
                result.put(shardName, new String(data));
            }
            return result;
        } catch (Exception e) {
            LOG.error("Unkown error while trying to create layout for table [" + table + "]", e);
            throw new BlurException("Unkown error while trying to create layout for table [" + table + "]");
        }
    }

    @Override
    public void process(WatchedEvent event) {
        shardServerHosts = updateNodeLists(NODE_TYPE.SHARD);
        controllerServerHosts = updateNodeLists(NODE_TYPE.CONTROLLER);
    }

    @Override
    public void registerNode(String hostName, NODE_TYPE type) {
        ZkUtils.mkNodesStr(zk, blurNodePath);
        ZkUtils.mkNodesStr(zk, blurNodePath + "/" + type.name());
        while (true) {
            int retry = 10;
            try {
                zk.create(blurNodePath + "/" + type.name() + "/" + hostName, null, Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                return;
            } catch (KeeperException e) {
                if (e.code().equals(Code.NODEEXISTS)) {
                    if (retry > 0) {
                        LOG.info("Waiting to register node [" + hostName + "] as type [" + type.name()
                                + "], probably because node was shutdown and restarted...");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            LOG.error("Unknown error waiting to register node.", ex);
                            throw new RuntimeException(ex);
                        }
                        retry--;
                        continue;
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Unknown error trying to register node.", e);
                throw new RuntimeException(e);
            }
        }
    }

    private List<String> updateNodeLists(NODE_TYPE type) {
        try {
            String path = blurNodePath + "/" + type.name();
            ZkUtils.mkNodesStr(zk, path);
            return new ArrayList<String>(zk.getChildren(path, this));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void create(String table, TableDescriptor desc) throws BlurException {
        if (tableList().contains(table)) {
            throw new BlurException("Table " + table + " already exists.");
        }
        desc.isEnabled = false;
        try {
            save(table, desc);
        } catch (Exception e) {
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public TableDescriptor describe(String table) throws BlurException {
        try {
            TableDescriptor tableDescriptor = get(table);
            if (tableDescriptor == null) {
                throw new BlurException("Table " + table + " does not exist.");
            }
            return tableDescriptor;
        } catch (Exception e) {
            throw new BlurException(e.getMessage());
        }
    }

    private TableDescriptor get(String table) throws KeeperException, InterruptedException, IOException,
            ClassNotFoundException {
        ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath, BLUR_TABLES_NODE));
        String path = ZkUtils.getPath(blurPath, BLUR_TABLES_NODE, table);
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            return null;
        } else {
            byte[] data = zk.getData(path, false, stat);
            return readTableDescriptor(data);
        }
    }

    private void save(String table, TableDescriptor descriptor) throws KeeperException, InterruptedException,
            IOException {
        ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath, BLUR_TABLES_NODE));
        String path = ZkUtils.getPath(blurPath, BLUR_TABLES_NODE, table);
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, writeTableDescriptor(descriptor), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(path, writeTableDescriptor(descriptor), stat.getVersion());
        }
    }

    private void remove(String table) throws InterruptedException, KeeperException, IOException {
        ZkUtils.mkNodesStr(zk, ZkUtils.getPath(blurPath, BLUR_TABLES_NODE));
        String path = ZkUtils.getPath(blurPath, BLUR_TABLES_NODE, table);
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            return;
        } else {
            zk.delete(path, stat.getVersion());
        }
    }

    private byte[] writeTableDescriptor(TableDescriptor descriptor) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(baos);
        String analyzerDef = descriptor.getAnalyzerDef();
        String partitionerClass = descriptor.getPartitionerClass();
        List<String> shardNames = descriptor.getShardNames();
        outputStream.writeBoolean(descriptor.isEnabled);
        outputStream.writeObject(analyzerDef);
        outputStream.writeObject(partitionerClass);
        outputStream.writeObject(shardNames);
        outputStream.close();
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private TableDescriptor readTableDescriptor(byte[] data) throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            TableDescriptor descriptor = new TableDescriptor();
            descriptor.isEnabled = inputStream.readBoolean();
            descriptor.analyzerDef = (String) inputStream.readObject();
            descriptor.partitionerClass = (String) inputStream.readObject();
            descriptor.shardNames = (List<String>) inputStream.readObject();
            return descriptor;
        } finally {
            inputStream.close();
        }
    }

    @Override
    public void enable(String table) throws BlurException {
        TableDescriptor descriptor = describe(table);
        if (descriptor.isEnabled) {
            return;
        }
        descriptor.isEnabled = true;
        try {
            save(table,descriptor);
        } catch (Exception e) {
            throw new BlurException(e.getMessage());
        }
        try {
            createAllTableShards(table,descriptor);
        } catch (IOException e) {
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public void disable(String table) throws BlurException {
        TableDescriptor descriptor = describe(table);
        descriptor.isEnabled = false;
        try {
            save(table,descriptor);
        } catch (Exception e) {
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public void drop(String table) throws BlurException {
        TableDescriptor descriptor = describe(table);
        if (descriptor.isEnabled) {
            throw new BlurException("Table " + table + " must be disabled before dropping.");
        }
        try {
            remove(table);
        } catch (Exception e) {
            throw new BlurException(e.getMessage());
        }
    }

    private void createAllTableShards(String table, TableDescriptor descriptor) throws IOException {
        mele.createDirectoryCluster(table);
        List<String> shardNames = descriptor.shardNames;
        for (String shard : shardNames) {
            mele.createDirectory(table, shard);
        }
    }

    @Override
    public List<String> getControllerServerHosts() {
        return controllerServerHosts;
    }

    @Override
    public List<String> getShardServerHosts() {
        return shardServerHosts;
    }

    @Override
    public Mele getMele() {
        return mele;
    }
}
