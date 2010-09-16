package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.manager.hits.HitsComparator;
import com.nearinfinity.blur.manager.hits.HitsPeekableIteratorComparator;
import com.nearinfinity.blur.manager.hits.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.ColumnFamily;
import com.nearinfinity.blur.thrift.generated.Hit;


public interface BlurConstants {
	public static final String BLUR_ZOOKEEPER_PATH = "blur.zookeeper.path";
	public static final String BLUR_ZOOKEEPER_PATH_DEFAULT = "/blur";
	public static final String BLUR_TABLES_NODE = "tables";
	public static final String BLUR_SERVER_CONTROLLER_PORT = "blur.server.controller.port";
	public static final String BLUR_SERVER_SHARD_PORT = "blur.server.shard.port";
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final Comparator<? super ColumnFamily> COLUMN_FAMILY_COMPARATOR = new ColumnFamilyComparator();
	public static final Comparator<? super Column> COLUMN_COMPARATOR = new ColumnComparator();
    public static final Comparator<? super PeekableIterator<Hit>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new HitsPeekableIteratorComparator();
    public static final Comparator<? super Hit> HITS_COMPARATOR = new HitsComparator();
}
