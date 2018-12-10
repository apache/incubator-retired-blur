/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

public class BlurBlockPlacementPolicyDefault extends BlockPlacementPolicyDefault {

  public static final String BLUR_BLOCK_PLACEMENT_SERVER_LOOKUP = "blur.block.placement.server.lookup";

  private static final Log LOG = LogFactory.getLog(BlurBlockPlacementPolicyDefault.class);

  private static final ServerLookup DEFAULT = new ServerLookup(null, null) {

    @Override
    public boolean isPathSupported(String srcPath) {
      return false;
    }

    @Override
    public String getShardServer(String srcPath) {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public DatanodeDescriptor getDatanodeDescriptor(String shardServer) {
      throw new RuntimeException("Not implemented.");
    }
  };

  private ServerLookup _serverLookup;
  private Random _random;

  @Override
  public void initialize(Configuration conf, FSClusterStats stats, NetworkTopology clusterMap,
      Host2NodesMap host2datanodeMap) {
    LOG.info("initialize");
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    _random = new Random();
    Class<?> c = conf.getClass(BLUR_BLOCK_PLACEMENT_SERVER_LOOKUP, DefaultServerLookup.class);
    if (host2datanodeMap == null) {
      _serverLookup = DEFAULT;
    } else {
      try {
        Constructor<?> constructor = c.getConstructor(new Class[] { Configuration.class, Host2NodesMap.class });
        _serverLookup = (ServerLookup) constructor.newInstance(conf, host2datanodeMap);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas, Node writer,
      List<DatanodeStorageInfo> chosenNodes, boolean returnChosenNodes, Set<Node> excludedNodes, long blocksize,
      BlockStoragePolicy storagePolicy) {
    LOG.info("chooseTarget");
    if (_serverLookup.isPathSupported(srcPath)) {
      String shardServer = _serverLookup.getShardServer(srcPath);
      if (shardServer == null) {
        return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
            blocksize, storagePolicy);
      }
      DatanodeDescriptor shardServerDatanodeDescriptor = _serverLookup.getDatanodeDescriptor(shardServer);
      if (shardServerDatanodeDescriptor == null) {
        return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
            blocksize, storagePolicy);
      }
      if (isAlreadyChosen(chosenNodes, shardServerDatanodeDescriptor)) {
        return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
            blocksize, storagePolicy);
      } else {
        DatanodeStorageInfo[] shardServerStorageInfos = shardServerDatanodeDescriptor.getStorageInfos();
        if (shardServerStorageInfos == null || shardServerStorageInfos.length == 0) {
          return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
              blocksize, storagePolicy);
        }
        DatanodeStorageInfo shardServerStorageInfo = choseOne(shardServerStorageInfos);
        if (numOfReplicas - 1 == 0) {
          if (returnChosenNodes) {
            List<DatanodeStorageInfo> copy = new ArrayList<DatanodeStorageInfo>(chosenNodes);
            copy.add(shardServerStorageInfo);
            return copy.toArray(new DatanodeStorageInfo[copy.size()]);
          } else {
            return new DatanodeStorageInfo[] { shardServerStorageInfo };
          }
        }
        if (chosenNodes == null) {
          chosenNodes = new ArrayList<DatanodeStorageInfo>();
        }
        chosenNodes.add(shardServerStorageInfo);
        if (returnChosenNodes) {
          return super.chooseTarget(srcPath, numOfReplicas - 1, writer, chosenNodes, returnChosenNodes, excludedNodes,
              blocksize, storagePolicy);
        } else {
          DatanodeStorageInfo[] datanodeStorageInfos = super.chooseTarget(srcPath, numOfReplicas - 1, writer,
              chosenNodes, returnChosenNodes, excludedNodes, blocksize, storagePolicy);
          DatanodeStorageInfo[] result = new DatanodeStorageInfo[datanodeStorageInfos.length + 1];
          System.arraycopy(datanodeStorageInfos, 0, result, 1, datanodeStorageInfos.length);
          result[0] = shardServerStorageInfo;
          return result;
        }
      }
    } else {
      return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
          blocksize, storagePolicy);
    }
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(String srcPath, LocatedBlock lBlk, int numberOfReplicas) {
    LOG.info("verifyBlockPlacement");
    if (_serverLookup.isPathSupported(srcPath)) {
      String shardServer = _serverLookup.getShardServer(srcPath);
      if (shardServer != null) {
        return super.verifyBlockPlacement(srcPath, lBlk, numberOfReplicas);
      }
      DatanodeDescriptor shardServerDatanodeDescriptor = _serverLookup.getDatanodeDescriptor(shardServer);
      String shardServerDatanodeUuid = shardServerDatanodeDescriptor.getDatanodeUuid();
      DatanodeInfo[] locations = lBlk.getLocations();
      for (DatanodeInfo info : locations) {
        String datanodeUuid = info.getDatanodeUuid();
        if (shardServerDatanodeUuid.equals(datanodeUuid)) {
          // then one of the locations is on the shard server.
          return super.verifyBlockPlacement(srcPath, lBlk, numberOfReplicas);
        }
      }
      // none of the replicas are on a shard server.
      BlockPlacementStatus blockPlacementStatus = super.verifyBlockPlacement(srcPath, lBlk, numberOfReplicas);
      if (blockPlacementStatus.isPlacementPolicySatisfied()) {
        // default block placement is satisfied, but we are not.
        return new BlurBlockPlacementStatusDefault(blockPlacementStatus, shardServer);
      } else {
        // both are unsatisfied
        return new BlurBlockPlacementStatusDefault(blockPlacementStatus, shardServer);
      }
    } else {
      return super.verifyBlockPlacement(srcPath, lBlk, numberOfReplicas);
    }
  }

  @Override
  public DatanodeStorageInfo chooseReplicaToDelete(BlockCollection bc, Block block, short replicationFactor,
      Collection<DatanodeStorageInfo> first, Collection<DatanodeStorageInfo> second, List<StorageType> excessTypes) {
    LOG.info("chooseReplicaToDelete rep [" + replicationFactor + "]");

    String path = bc.getName();

    String shardServer = _serverLookup.getShardServer(path);
    if (shardServer == null) {
      return super.chooseReplicaToDelete(bc, block, replicationFactor, first, second, excessTypes);
    }
    DatanodeDescriptor shardServerDatanodeDescriptor = _serverLookup.getDatanodeDescriptor(shardServer);

    if (replicationFactor > 1) {
      Collection<DatanodeStorageInfo> firstCopy = new ArrayList<DatanodeStorageInfo>();
      for (DatanodeStorageInfo info : first) {
        DatanodeDescriptor datanodeDescriptor = info.getDatanodeDescriptor();
        if (!datanodeDescriptor.equals(shardServerDatanodeDescriptor)) {
          firstCopy.add(info);
        }
      }

      Collection<DatanodeStorageInfo> secondCopy = new ArrayList<DatanodeStorageInfo>();
      for (DatanodeStorageInfo info : second) {
        DatanodeDescriptor datanodeDescriptor = info.getDatanodeDescriptor();
        if (!datanodeDescriptor.equals(shardServerDatanodeDescriptor)) {
          secondCopy.add(info);
        }
      }

      return super.chooseReplicaToDelete(bc, block, replicationFactor, firstCopy, secondCopy, excessTypes);
    } else {
      for (DatanodeStorageInfo info : first) {
        DatanodeDescriptor datanodeDescriptor = info.getDatanodeDescriptor();
        if (!datanodeDescriptor.equals(shardServerDatanodeDescriptor)) {
          return info;
        }
      }

      for (DatanodeStorageInfo info : second) {
        DatanodeDescriptor datanodeDescriptor = info.getDatanodeDescriptor();
        if (!datanodeDescriptor.equals(shardServerDatanodeDescriptor)) {
          return info;
        }
      }
      throw new RuntimeException("Should never happen!!!");
    }
  }

  private DatanodeStorageInfo choseOne(DatanodeStorageInfo[] storageInfos) {
    synchronized (_random) {
      return storageInfos[_random.nextInt(storageInfos.length)];
    }
  }

  private boolean isAlreadyChosen(List<DatanodeStorageInfo> chosenNodes, DatanodeDescriptor datanodeDescriptor) {
    for (DatanodeStorageInfo info : chosenNodes) {
      if (info.getDatanodeDescriptor().equals(datanodeDescriptor)) {
        return true;
      }
    }
    return false;
  }
}
