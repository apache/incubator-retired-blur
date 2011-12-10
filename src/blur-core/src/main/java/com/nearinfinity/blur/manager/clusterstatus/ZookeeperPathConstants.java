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


public class ZookeeperPathConstants {
  
  public static String getBasePath() {
    return "/blur";
  }

  public static String getClusterPath(String cluster) {
    return getClustersPath() + "/" + cluster;
  }

  public static String getClustersPath() {
    return getBasePath() + "/clusters";
  }

  public static String getOnlineControllersPath() {
    return getBasePath() + "/online-controller-nodes";
  }

  public static String getTableEnabledPath(String cluster, String table) {
    return getTablePath(cluster,table) + "/enabled";
  }

  public static String getTableUriPath(String cluster, String table) {
    return getTablePath(cluster,table) + "/uri";
  }

  public static String getTableShardCountPath(String cluster, String table) {
    return getTablePath(cluster,table) + "/shard-count";
  }

  public static String getOnlinePath(String cluster) {
    return getClusterPath(cluster) + "/online";
  }

  public static String getOnlineShardsPath(String cluster) {
    return getOnlinePath(cluster) + "/shard-nodes";
  }
  
  public static String getTablesPath(String cluster) {
    return getClusterPath(cluster) + "/tables";
  }

  public static String getTablePath(String cluster, String table) {
    return getTablesPath(cluster) + "/" + table;
  }

  public static String getSafemodePath(String cluster) {
    return getClusterPath(cluster) + "/safemode";
  }

  public static String getRegisteredShardsPath(String cluster) {
    return getClusterPath(cluster) + "/shard-nodes";
  }

  public static String getTableCompressionCodecPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/compression-codec";
  }

  public static String getTableCompressionBlockSizePath(String cluster, String table) {
    return getTablePath(cluster, table) + "/compression-blocksize";
  }

  public static String getLockPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/locks";
  }

  public static String getTableBlockCachingFileTypesPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/blockcachingfiletypes";
  }

  public static String getTableBlockCachingPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/blockcaching";
  }

  public static String getTableSimilarityPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/similarity";
  }

  public static String getTableFieldNamesPath(String cluster, String table) {
    return getTablePath(cluster, table) + "/fieldnames";
  }
  
  public static String getTableFieldNamesPath(String cluster, String table, String fieldName) {
    return getTableFieldNamesPath(cluster, table) + "/" + fieldName;
  }

}
