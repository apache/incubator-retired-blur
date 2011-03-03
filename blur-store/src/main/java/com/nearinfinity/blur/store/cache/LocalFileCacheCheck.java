package com.nearinfinity.blur.store.cache;

import java.io.IOException;

public interface LocalFileCacheCheck {

    boolean isSafeForRemoval(String table, String shard, String name) throws IOException;

}
