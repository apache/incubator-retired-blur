package org.apache.blur.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;

public class BlurConstants {

  public static final String CONTROLLER = "controller";
  public static final String SHARD = "shard";
  public static final String SHARD_PREFIX = "shard-";

  public static final String PRIME_DOC = "_prime_";
  public static final String PRIME_DOC_VALUE = "true";
  public static final String ROW_ID = "rowid";
  public static final String RECORD_ID = "recordid";
  public static final String FAMILY = "family";
  public static final String SUPER = "super";
  public static final String SEP = ".";

  public static final String BLUR_TABLE_PATH = "blur.table.path";
  public static final String BLUR_ZOOKEEPER_CONNECTION = "blur.zookeeper.connection";
  public static final String BLUR_ZOOKEEPER_TIMEOUT = "blur.zookeeper.timeout";
  public static final int BLUR_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  public static final String BLUR_SHARD_HOSTNAME = "blur.shard.hostname";
  public static final String BLUR_SHARD_BIND_PORT = "blur.shard.bind.port";
  public static final String BLUR_SHARD_BIND_ADDRESS = "blur.shard.bind.address";
  public static final String BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION = "blur.shard.blockcache.direct.memory.allocation";
  public static final String BLUR_SHARD_BLOCKCACHE_SLAB_COUNT = "blur.shard.blockcache.slab.count";
  public static final String BLUR_SHARD_BLOCK_CACHE_VERSION = "blur.shard.block.cache.version";
  public static final String BLUR_SHARD_SAFEMODEDELAY = "blur.shard.safemodedelay";
  public static final String BLUR_CONTROLLER_HOSTNAME = "blur.controller.hostname";
  public static final String BLUR_CONTROLLER_BIND_PORT = "blur.controller.bind.port";
  public static final String BLUR_CONTROLLER_BIND_ADDRESS = "blur.controller.bind.address";
  public static final String BLUR_QUERY_MAX_ROW_FETCH = "blur.query.max.row.fetch";
  public static final String BLUR_QUERY_MAX_RECORD_FETCH = "blur.query.max.record.fetch";
  public static final String BLUR_QUERY_MAX_RESULTS_FETCH = "blur.query.max.results.fetch";
  public static final String BLUR_SHARD_FETCHCOUNT = "blur.shard.fetchcount";
  public static final String BLUR_MAX_HEAP_PER_ROW_FETCH = "blur.max.heap.per.row.fetch";
  public static final String BLUR_MAX_RECORDS_PER_ROW_FETCH_REQUEST = "blur.max.records.per.row.fetch.request";

  public static final String BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT = "blur.shard.server.thrift.thread.count";
  public static final String BLUR_SHARD_CACHE_MAX_TIMETOLIVE = "blur.shard.cache.max.timetolive";
  public static final String BLUR_SHARD_FILTER_CACHE_CLASS = "blur.shard.filter.cache.class";
  public static final String BLUR_SHARD_INDEX_WARMUP_CLASS = "blur.shard.index.warmup.class";
  public static final String BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT = "blur.indexmanager.search.thread.count";
  public static final String BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT = "blur.indexmanager.mutate.thread.count";
  public static final String BLUR_SHARD_DATA_FETCH_THREAD_COUNT = "blur.shard.data.fetch.thread.count";
  public static final String BLUR_SHARD_INTERNAL_SEARCH_THREAD_COUNT = "blur.shard.internal.search.thread.count";
  public static final String BLUR_SHARD_WARMUP_THREAD_COUNT = "blur.shard.warmup.thread.count";
  public static final String BLUR_SHARD_INDEX_WARMUP_THROTTLE = "blur.shard.index.warmup.throttle";
  public static final String BLUR_MAX_CLAUSE_COUNT = "blur.max.clause.count";
  public static final String BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS = "blur.shard.cache.max.querycache.elements";
  public static final String BLUR_SHARD_OPENER_THREAD_COUNT = "blur.shard.opener.thread.count";
  public static final String BLUR_SHARD_MERGE_THREAD_COUNT = "blur.shard.merge.thread.count";
  public static final String BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE = "blur.shard.index.deletion.policy.maxage";
  public static final String BLUR_ZOOKEEPER_SYSTEM_TIME_TOLERANCE = "blur.zookeeper.system.time.tolerance";
  public static final String BLUR_SHARD_INDEX_SIMILARITY = "blur.shard.index.similarity";
  public static final String BLUR_SHARD_THRIFT_SELECTOR_THREADS = "blur.shard.thrift.selector.threads";
  public static final String BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES = "blur.shard.thrift.max.read.buffer.bytes";
  public static final String BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD = "blur.shard.thrift.accept.queue.size.per.thread";
  public static final String BLUR_SHARD_DISTRIBUTED_LAYOUT_FACTORY_CLASS = "blur.shard.distributed.layout.factory.class";
  
  public static final String BLUR_FIELDTYPE = "blur.fieldtype.";

  public static final String BLUR_SHARD_TIME_BETWEEN_COMMITS = "blur.shard.time.between.commits";
  public static final String BLUR_SHARD_TIME_BETWEEN_REFRESHS = "blur.shard.time.between.refreshs";

  public static final String BLUR_CONTROLLER_SERVER_THRIFT_THREAD_COUNT = "blur.controller.server.thrift.thread.count";
  public static final String BLUR_CONTROLLER_SERVER_REMOTE_THREAD_COUNT = "blur.controller.server.remote.thread.count";
  public static final String BLUR_CONTROLLER_REMOTE_FETCH_COUNT = "blur.controller.remote.fetch.count";
  
  public static final String BLUR_CONTROLLER_SHARD_CONNECTION_TIMEOUT = "blur.controller.shard.connection.timeout";
  public static final String BLUR_CONTROLLER_RETRY_MAX_MUTATE_RETRIES = "blur.controller.retry.max.mutate.retries";
  public static final String BLUR_CONTROLLER_RETRY_MAX_DEFAULT_RETRIES = "blur.controller.retry.max.default.retries";
  public static final String BLUR_CONTROLLER_RETRY_FETCH_DELAY = "blur.controller.retry.fetch.delay";
  public static final String BLUR_CONTROLLER_RETRY_DEFAULT_DELAY = "blur.controller.retry.default.delay";
  public static final String BLUR_CONTROLLER_RETRY_MUTATE_DELAY = "blur.controller.retry.mutate.delay";
  public static final String BLUR_CONTROLLER_RETRY_MAX_FETCH_DELAY = "blur.controller.retry.max.fetch.delay";
  public static final String BLUR_CONTROLLER_RETRY_MAX_MUTATE_DELAY = "blur.controller.retry.max.mutate.delay";
  public static final String BLUR_CONTROLLER_RETRY_MAX_DEFAULT_DELAY = "blur.controller.retry.max.default.delay";
  public static final String BLUR_CONTROLLER_RETRY_MAX_FETCH_RETRIES = "blur.controller.retry.max.fetch.retries";
  public static final String BLUR_CONTROLLER_THRIFT_SELECTOR_THREADS = "blur.controller.thrift.selector.threads";
  public static final String BLUR_CONTROLLER_THRIFT_MAX_READ_BUFFER_BYTES = "blur.controller.thrift.max.read.buffer.bytes";
  public static final String BLUR_CONTROLLER_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD = "blur.controller.thrift.accept.queue.size.per.thread";
  public static final String BLUR_CLIENTPOOL_CLIENT_CLOSE_THRESHOLD = "blur.clientpool.client.close.threshold";
  public static final String BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY = "blur.clientpool.client.clean.frequency";
  
  public static final String BLUR_GUI_CONTROLLER_PORT = "blur.gui.controller.port";
  public static final String BLUR_GUI_SHARD_PORT = "blur.gui.shard.port";

  public static final String DEFAULT = "default";
  public static final String BLUR_CLUSTER_NAME = "blur.cluster.name";
  public static final String BLUR_CLUSTER;

  public static final long ZK_WAIT_TIME = TimeUnit.SECONDS.toMillis(5);
  public static final String DELETE_MARKER_VALUE = "delete";
  public static final String DELETE_MARKER = "_deletemarker_";
  
  public static final String SHARED_MERGE_SCHEDULER = "sharedMergeScheduler";

  static {
    try {
      BlurConfiguration configuration = new BlurConfiguration();
      BLUR_CLUSTER = configuration.get(BLUR_CLUSTER_NAME, DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException("Unknown error parsing configuration.", e);
    }
  }

  public static String getDefaultTableUriPropertyName(String cluster) {
    return "blur.cluster." + cluster + ".table.uri";
  }
}
