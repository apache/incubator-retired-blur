package com.nearinfinity.blur.store;

import java.io.IOException;

public interface ExistenceCheck {

    boolean existsInBase(String dirName, String name) throws IOException;

}
