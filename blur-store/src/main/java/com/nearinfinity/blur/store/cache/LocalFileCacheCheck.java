package com.nearinfinity.blur.store.cache;

import java.io.IOException;

public interface LocalFileCacheCheck {

    boolean isBeingServed(String dirName, String name) throws IOException;

}
