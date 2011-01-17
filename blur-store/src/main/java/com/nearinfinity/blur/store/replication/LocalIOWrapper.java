package com.nearinfinity.blur.store.replication;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public interface LocalIOWrapper {

    IndexOutput wrapOutput(IndexOutput fileIndexOutput);

    IndexInput wrapInput(IndexInput fileIndexInput);

}
