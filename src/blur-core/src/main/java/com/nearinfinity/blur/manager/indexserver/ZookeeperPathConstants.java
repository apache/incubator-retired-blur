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

import java.io.IOException;

import com.nearinfinity.blur.BlurConfiguration;

public class ZookeeperPathConstants {
    
    private static final String DEFAULT = "default";
    private static final String BLUR_CLUSTER_NAME = "blur.cluster.name";
    private static final String BLUR_BASE_PATH                  = "/blur/" + getClusterName();
    
    private static final String BLUR_TABLES_ENABLED             = "enabled";
    // /blur/tables/<name>/enabled will indicate that the table is enabled
    private static final String BLUR_TABLES_URI                 = "uri";
    private static final String BLUR_TABLES_SHARD_COUNT         = "shard-count";
    
    private static final String BLUR_ONLINE_PATH                = BLUR_BASE_PATH + "/online";
    private static final String BLUR_ONLINE_SHARDS_PATH         = BLUR_BASE_PATH + "/online/shard-nodes";
    private static final String BLUR_ONLINE_CONTROLLERS_PATH    = BLUR_BASE_PATH + "/online/controller-nodes";
    private static final String BLUR_TABLES                     = BLUR_BASE_PATH + "/tables";
    
    private static final String BLUR_SAFEMODE                   = BLUR_BASE_PATH + "/safemode";
    private static final String BLUR_SAFEMODE_LOCK              = BLUR_BASE_PATH + "/safemode/lock";
    private static final String BLUR_SAFEMODE_SHUTDOWN          = BLUR_BASE_PATH + "/safemode/shutdown";
    private static final String BLUR_REGISTERED_SHARDS_PATH     = BLUR_BASE_PATH + "/shard-nodes";
    
    public static String getClusterName() {
        try {
            BlurConfiguration configuration = new BlurConfiguration();
            return configuration.get(BLUR_CLUSTER_NAME, DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Unknown error parsing configuration.",e);
        }
    }
    
    public static String getBlurBasePath() {
        return BLUR_BASE_PATH;
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
    public static String getBlurOnlineControllersPath() {
        return BLUR_ONLINE_CONTROLLERS_PATH;
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
    
    

}
