package com.nearinfinity.blur.manager.indexserver;

public interface ZookeeperPathContants {
    
    public static final String BLUR_BASE_PATH                  = "/blur";
    public static final String BLUR_ONLINE_PATH                = "/blur/online";
    public static final String BLUR_ONLINE_SHARDS_PATH         = "/blur/online/shard-nodes";
    public static final String BLUR_ONLINE_CONTROLLERS_PATH    = "/blur/online/controller-nodes";
    public static final String BLUR_TABLES                     = "/blur/tables";
    public static final String BLUR_TABLES_ENABLED             = "enabled";
    // /blur/tables/<name>/enabled will indicate that the table is enabled
    
    public static final String BLUR_SAFEMODE                   = "/blur/safemode";
    public static final String BLUR_SAFEMODE_LOCK              = "/blur/safemode/lock";
    public static final String BLUR_REGISTERED_SHARDS_PATH     = "/blur/shard-nodes";

}
