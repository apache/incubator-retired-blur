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

package com.nearinfinity.blur.manager.clusterstatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.nearinfinity.blur.thrift.generated.TableDescriptor;

public abstract class ClusterStatus {

  public abstract List<String> getOnlineShardServers(boolean useCache, String cluster);

  public abstract List<String> getControllerServerList();

  public abstract List<String> getShardServerList(String cluster);

  public abstract List<String> getClusterList(boolean useCache);

  public abstract TableDescriptor getTableDescriptor(boolean useCache, String cluster, String table);

  public final List<String> getTableList(boolean useCache) {
    List<String> tables = new ArrayList<String>();
    for (String cluster : getClusterList(useCache)) {
      tables.addAll(getTableList(useCache, cluster));
    }
    return tables;
  }

  public abstract String getCluster(boolean useCache, String table);

  public abstract boolean isEnabled(boolean useCache, String cluster, String table);

  public abstract boolean exists(boolean useCache, String cluster, String table);

  public abstract boolean isInSafeMode(boolean useCache, String cluster);

  public List<String> getOfflineShardServers(boolean useCache, String cluster) {
    List<String> shardServerList = new ArrayList<String>(getShardServerList(cluster));
    shardServerList.removeAll(getOnlineShardServers(useCache, cluster));
    return shardServerList;
  }

  public abstract int getShardCount(boolean useCache, String cluster, String table);

  public abstract boolean isBlockCacheEnabled(String cluster, String table);

  public abstract Set<String> getBlockCacheFileTypes(String cluster, String table);

  public abstract List<String> getTableList(boolean useCache, String cluster);

  public abstract boolean isReadOnly(boolean useCache, String cluster, String table);

  public abstract void createTable(TableDescriptor tableDescriptor);

  public abstract void disableTable(String cluster, String table);

  public abstract void enableTable(String cluster, String table);

  public abstract void removeTable(String cluster, String table, boolean deleteIndexFiles);

  public abstract boolean isOpen();

}
