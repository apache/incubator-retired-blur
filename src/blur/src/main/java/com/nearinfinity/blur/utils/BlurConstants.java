package com.nearinfinity.blur.utils;

import java.util.Comparator;

import com.nearinfinity.blur.thrift.generated.Column;
import com.nearinfinity.blur.thrift.generated.SuperColumn;


public interface BlurConstants {
	public static final String BLUR_DIRECTORY_MANAGER_STORE_CLASS = "blur.directory.manager.store.class";
	public static final String BLUR_ZOOKEEPER_PATH = "blur.zookeeper.path";
	public static final String BLUR_SHARDS_TOSERVE_PER_PASS = "blur.shards.toserve.per.pass";
	public static final String BLUR_LUCENE_SIMILARITY_CLASS = "blur.lucene.similarity.class";
	public static final String BLUR_TABLE_LOCKS_NODE = "tableLocks";
	public static final String BLUR_TABLES_NODE = "tables";
	public static final String BLUR_SERVER_CONTROLLER_PORT = "blur.server.controller.port";
	public static final String BLUR_SERVER_SHARD_PORT = "blur.server.shard.port";
	public static final String BLUR_FILTER_MANAGER_CLASS = "blur.filter.manager.class";
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final Comparator<? super SuperColumn> SUPER_COLUMN_COMPARATOR = new SuperColumnComparator();
	public static final Comparator<? super Column> COLUMN_COMPARATOR = new ColumnComparator();
}
