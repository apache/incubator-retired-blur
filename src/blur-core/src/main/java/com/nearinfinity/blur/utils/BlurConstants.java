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

package com.nearinfinity.blur.utils;

import java.io.IOException;
import java.util.Comparator;

import com.nearinfinity.blur.BlurConfiguration;
import com.nearinfinity.blur.manager.results.BlurResultComparator;
import com.nearinfinity.blur.manager.results.BlurResultPeekableIteratorComparator;
import com.nearinfinity.blur.manager.results.PeekableIterator;
import com.nearinfinity.blur.thrift.generated.BlurResult;


public class BlurConstants {
	
	public static final String CONTROLLER = "controller";
	public static final String SHARD = "shard";
	public static final String SHARD_PREFIX = "shard-";
    public static final Comparator<? super PeekableIterator<BlurResult>> HITS_PEEKABLE_ITERATOR_COMPARATOR = new BlurResultPeekableIteratorComparator();
    public static final Comparator<? super BlurResult> HITS_COMPARATOR = new BlurResultComparator();

    public static final String PRIME_DOC = "_prime_";
    public static final String PRIME_DOC_VALUE = "true";
    public static final String ROW_ID = "rowid";
    public static final String RECORD_ID = "recordid";
    public static final String SUPER = "super";
    public static final String SEP = ".";
    
    public static final String BLUR_LOCAL_CACHE_PATHS = "blur.local.cache.paths";
    public static final String BLUR_TABLE_PATH = "blur.table.path";
    public static final String BLUR_ZOOKEEPER_CONNECTION = "blur.zookeeper.connection";
    public static final String BLUR_SHARD_HOSTNAME = "blur.shard.hostname";
    public static final String BLUR_SHARD_BIND_PORT = "blur.shard.bind.port";
    public static final String BLUR_SHARD_BIND_ADDRESS = "blur.shard.bind.address";
    public static final String BLUR_CONTROLLER_HOSTNAME = "blur.controller.hostname";
    public static final String BLUR_CONTROLLER_BIND_PORT = "blur.controller.bind.port";
    public static final String BLUR_CONTROLLER_BIND_ADDRESS = "blur.controller.bind.address";
    public static final String CRAZY = "CRAZY";
    
    public static final String BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT = "blur.shard.server.thrift.thread.count";
    public static final String BLUR_SHARD_CACHE_MAX_TIMETOLIVE = "blur.shard.cache.max.timetolive";
    public static final String BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT = "blur.indexmanager.search.thread.count";
    public static final String BLUR_MAX_CLAUSE_COUNT = "blur.max.clause.count";
    public static final String BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS = "blur.shard.cache.max.querycache.elements";
    public static final String BLUR_SHARD_OPENER_THREAD_COUNT = "blur.shard.opener.thread.count";
    public static final String BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE = "blur.zookeeper.system.time.tolerance";
    
    public static final String BLUR_CONTROLLER_SERVER_THRIFT_THREAD_COUNT = "blur.controller.server.thrift.thread.count";
    public static final String BLUR_CONTROLLER_CACHE_MAX_TIMETOLIVE = "blur.controller.cache.max.timetolive";
    public static final String BLUR_CONTROLLER_CACHE_MAX_QUERYCACHE_ELEMENTS = "blur.controller.cache.max.querycache.elements";
    public static final String BLUR_CONTROLLER_REMOTE_FETCH_COUNT = "blur.controller.remote.fetch.count";
    
    public static final String DEFAULT                            = "default";
    public static final String BLUR_CLUSTER_NAME                  = "blur.cluster.name";
    public static final String BLUR_CLUSTER;
    
    static {
        try {
            BlurConfiguration configuration = new BlurConfiguration();
            BLUR_CLUSTER = configuration.get(BLUR_CLUSTER_NAME, DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Unknown error parsing configuration.",e);
        }
    }
}
