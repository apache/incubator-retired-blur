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

package com.nearinfinity.blur.manager.indexserver;

import com.nearinfinity.blur.utils.BlurConstants;

public class ZookeeperPathConstants {

  private static final String BLUR_TABLES_ENABLED = "enabled";
  // /blur/tables/<name>/enabled will indicate that the table is enabled
  private static final String BLUR_TABLES_URI = "uri";
  private static final String BLUR_TABLES_SHARD_COUNT = "shard-count";
  private static final String BLUR_TABLES_COMPRESSION_CODEC = "compression-codec";
  private static final String BLUR_TABLES_COMPRESSION_BLOCK_SIZE = "compression-blocksize";

  private static final String BLUR_ONLINE_PATH = getBlurBasePath() + "/online";
  private static final String BLUR_ONLINE_SHARDS_PATH = getBlurBasePath() + "/online/shard-nodes";
  private static final String BLUR_TABLES = getBlurBasePath() + "/tables";
  private static final String BLUR_TABLES_LOCKS = getBlurBasePath() + "/tables-locks";

  private static final String BLUR_SAFEMODE = getBlurBasePath() + "/safemode";
  private static final String BLUR_SAFEMODE_LOCK = getBlurBasePath() + "/safemode/lock";
  private static final String BLUR_SAFEMODE_SHUTDOWN = getBlurBasePath() + "/safemode/shutdown";
  private static final String BLUR_REGISTERED_SHARDS_PATH = getBlurBasePath() + "/shard-nodes";
  private static final String BLUR_TABLE_LOCK_PATH = "locks";

  public static String getBlurBasePath() {
    return "/blur/clusters/" + BlurConstants.BLUR_CLUSTER;
  }

  public static String getBlurClusterPath() {
    return "/blur/clusters";
  }

  public static String getBlurOnlineControllersPath() {
    return "/blur/online-controller-nodes";
  }

  public static String getBlurTablesEnabled() {
    return BLUR_TABLES_ENABLED;
  }

  public static String getBlurTablesUri() {
    return BLUR_TABLES_URI;
  }

  public static String getBlurTablesShardCount() {
    return BLUR_TABLES_SHARD_COUNT;
  }

  public static String getBlurOnlinePath() {
    return BLUR_ONLINE_PATH;
  }

  public static String getBlurOnlineShardsPath() {
    return BLUR_ONLINE_SHARDS_PATH;
  }

  public static String getBlurTablesPath() {
    return BLUR_TABLES;
  }

  public static String getBlurSafemodePath() {
    return BLUR_SAFEMODE;
  }

  public static String getBlurSafemodeLockPath() {
    return BLUR_SAFEMODE_LOCK;
  }

  public static String getBlurSafemodeShutdownPath() {
    return BLUR_SAFEMODE_SHUTDOWN;
  }

  public static String getBlurRegisteredShardsPath() {
    return BLUR_REGISTERED_SHARDS_PATH;
  }

  public static String getBlurTablesLocksPath() {
    return BLUR_TABLES_LOCKS;
  }

  public static String getBlurTablesCompressionCodec() {
    return BLUR_TABLES_COMPRESSION_CODEC;
  }

  public static String getBlurTablesCompressionBlockSize() {
    return BLUR_TABLES_COMPRESSION_BLOCK_SIZE;
  }

  public static String getBlurLockPath(String table) {
    return getBlurTablesPath() + "/" + table + "/" + BLUR_TABLE_LOCK_PATH;
  }
}
