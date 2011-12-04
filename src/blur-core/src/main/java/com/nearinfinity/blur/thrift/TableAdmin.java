package com.nearinfinity.blur.thrift;

import org.apache.thrift.TException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.manager.clusterstatus.ClusterStatus;
import com.nearinfinity.blur.manager.indexserver.utils.CreateTable;
import com.nearinfinity.blur.manager.indexserver.utils.DisableTable;
import com.nearinfinity.blur.manager.indexserver.utils.EnableTable;
import com.nearinfinity.blur.manager.indexserver.utils.RemoveTable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public abstract class TableAdmin implements Iface {

  private static final Log LOG = LogFactory.getLog(TableAdmin.class);
  protected ZooKeeper _zookeeper;
  protected ClusterStatus _clusterStatus;

  @Override
  public void createTable(TableDescriptor tableDescriptor) throws BlurException, TException {
    try {
      //@todo Remove this once issue #27 is resolved
      if (tableDescriptor.compressionBlockSize > 32768) {
        tableDescriptor.compressionBlockSize = 32768;
      } else if (tableDescriptor.compressionBlockSize < 8192) {
        tableDescriptor.compressionBlockSize = 8192;
      }
      CreateTable.createTable(_zookeeper, tableDescriptor);
    } catch (Exception e) {
      LOG.error("Unknown error during create of [table={0}, tableDescriptor={1}]", e, tableDescriptor.name, tableDescriptor);
      throw new BException(e.getMessage(), e);
    }
    if (tableDescriptor.isEnabled) {
      enableTable(tableDescriptor.name);
    }
  }

  @Override
  public void disableTable(String table) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(table);
      DisableTable.disableTable(_zookeeper, cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error during disable of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void enableTable(String table) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(table);
      EnableTable.enableTable(_zookeeper, cluster, table);
    } catch (Exception e) {
      LOG.error("Unknown error during enable of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  @Override
  public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
    try {
      String cluster = _clusterStatus.getCluster(table);
      RemoveTable.removeTable(_zookeeper, cluster, table, deleteIndexFiles);
    } catch (Exception e) {
      LOG.error("Unknown error during remove of [table={0}]", e, table);
      throw new BException(e.getMessage(), e);
    }
  }

  public void setZookeeper(ZooKeeper zookeeper) {
    _zookeeper = zookeeper;
  }
  
  public boolean isTableEnabled(boolean useCache, String table) {
    return _clusterStatus.isEnabled(useCache,table);
  }
  
  public void checkTable(String table) throws BlurException {
    if (inSafeMode(true,table)) {
      throw new BlurException("Cluster for [" + table + "] is in safe mode",null);
    }
    if (tableExists(true,table)) {
      if (isTableEnabled(true,table)) {
        return;
      }
      throw new BlurException("Table [" + table + "] exists, but is not enabled",null);
    } else {
      throw new BlurException("Table [" + table + "] does not exist",null);
    }
  }

  private boolean inSafeMode(boolean useCache, String table) {
    String cluster = _clusterStatus.getCluster(table);
    return _clusterStatus.isInSafeMode(cluster);
  }

  public boolean tableExists(boolean useCache, String table) {
    return _clusterStatus.exists(useCache,table);
  }
  
  public ClusterStatus getClusterStatus() {
    return _clusterStatus;
  }

  public void setClusterStatus(ClusterStatus clusterStatus) {
    _clusterStatus = clusterStatus;
  }
}
