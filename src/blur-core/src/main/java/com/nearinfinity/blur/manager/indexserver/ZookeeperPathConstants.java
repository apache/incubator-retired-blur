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

public class ZookeeperPathConstants {
    
    public static final String BLUR_BASE_PATH                  = "/blur";
    public static final String BLUR_ONLINE_PATH                = "/blur/online";
    public static final String BLUR_ONLINE_SHARDS_PATH         = "/blur/online/shard-nodes";
    public static final String BLUR_ONLINE_CONTROLLERS_PATH    = "/blur/online/controller-nodes";
    public static final String BLUR_TABLES                     = "/blur/tables";
    public static final String BLUR_TABLES_ENABLED             = "enabled";
    // /blur/tables/<name>/enabled will indicate that the table is enabled
    public static final String BLUR_TABLES_URI                 = "uri";
    
    public static final String BLUR_SAFEMODE                   = "/blur/safemode";
    public static final String BLUR_SAFEMODE_LOCK              = "/blur/safemode/lock";
    public static final String BLUR_SAFEMODE_SHUTDOWN          = "/blur/safemode/shutdown";
    public static final String BLUR_REGISTERED_SHARDS_PATH     = "/blur/shard-nodes";

}
