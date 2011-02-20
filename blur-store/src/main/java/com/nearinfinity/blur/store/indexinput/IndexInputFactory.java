package com.nearinfinity.blur.store.indexinput;

import java.io.IOException;

import com.nearinfinity.blur.store.replication.LocalIOWrapper;
import com.nearinfinity.blur.store.replication.ReplicationDaemon.RepliaWorkUnit;

public interface IndexInputFactory {

    void replicationComplete(RepliaWorkUnit workUnit, LocalIOWrapper wrapper, int bufferSize) throws IOException;

}
