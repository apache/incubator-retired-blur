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
  public static final String FIELDS = "_fields_";
  public static final String FAMILY = "family";
  public static final String DEFAULT_FAMILY = "_default_";
  public static final String SUPER = "super";
  public static final String SEP = ".";

  public static final String BLUR_SHARD_QUEUE_MAX_PAUSE_TIME_WHEN_EMPTY = "blur.shard.queue.max.pause.time.when.empty";
  public static final String BLUR_SHARD_QUEUE_MAX_WRITER_LOCK_TIME = "blur.shard.queue.max.writer.lock.time";
  public static final String BLUR_SHARD_QUEUE_MAX_QUEUE_BATCH_SIZE = "blur.shard.queue.max.queue.batch.size";
  public static final String BLUR_SHARD_QUEUE_MAX_INMEMORY_LENGTH = "blur.shard.queue.max.inmemory.length";

  public static final String BLUR_RECORD_SECURITY = "blur.record.security";
  public static final String BLUR_RECORD_SECURITY_DEFAULT_READMASK_MESSAGE = "blur.record.security.default.readmask.message";
  public static final String ACL_DISCOVER = "acl-discover";
  public static final String ACL_READ = "acl-read";

  public static final String FAST_DECOMPRESSION = "FAST_DECOMPRESSION";
  public static final String FAST = "FAST";
  public static final String HIGH_COMPRESSION = "HIGH_COMPRESSION";
  public static final String BLUR_SHARD_INDEX_CHUNKSIZE = "blur.shard.index.chunksize";
  public static final String BLUR_SHARD_INDEX_COMPRESSIONMODE = "blur.shard.index.compressionmode";

  // public static final String BLUR_TABLE_PATH = "blur.table.path";
  public static final String BLUR_ZOOKEEPER_CONNECTION = "blur.zookeeper.connection";
  public static final String BLUR_HDFS_TRACE_PATH = "blur.hdfs.trace.path";
  public static final String BLUR_ZOOKEEPER_TIMEOUT = "blur.zookeeper.timeout";
  public static final int BLUR_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  public static final String BLUR_SHARD_HOSTNAME = "blur.shard.hostname";
  public static final String BLUR_SHARD_BIND_PORT = "blur.shard.bind.port";
  public static final String BLUR_SHARD_BIND_ADDRESS = "blur.shard.bind.address";
  public static final String BLUR_SHARD_BLOCKCACHE_DIRECT_MEMORY_ALLOCATION = "blur.shard.blockcache.direct.memory.allocation";
  public static final String BLUR_SHARD_BLOCKCACHE_SLAB_COUNT = "blur.shard.blockcache.slab.count";
  public static final String BLUR_SHARD_BLOCK_CACHE_VERSION = "blur.shard.block.cache.version";
  public static final String BLUR_SHARD_BLOCK_CACHE_TOTAL_SIZE = "blur.shard.block.cache.total.size";
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
  public static final String BLUR_SHARD_READ_INTERCEPTOR = "blur.shard.read.interceptor";
  public static final String BLUR_SHARD_INTERNAL_SEARCH_THREAD_COUNT = "blur.shard.internal.search.thread.count";
  public static final String BLUR_SHARD_INDEX_WRITER_SORT_MEMORY = "blur.shard.index.writer.sort.memory";
  public static final String BLUR_SHARD_INDEX_WRITER_SORT_FACTOR = "blur.shard.index.writer.sort.factor";
  public static final String BLUR_SHARD_INDEX_MAX_IDLE_TIME = "blur.shard.index.max.idle.time";
  public static final String BLUR_TABLE_DISABLE_FAST_DIR = "blur.table.disable.fast.dir";
  public static final String BLUR_BULK_UPDATE_WORKING_PATH = "blur.bulk.update.working.path";
  public static final String BLUR_BULK_UPDATE_WORKING_PATH_PERMISSION = "blur.bulk.update.working.path.permission";

  public static final String BLUR_SHARD_SERVER_THRIFT_THREAD_COUNT = "blur.shard.server.thrift.thread.count";
  public static final String BLUR_SHARD_CACHE_MAX_TIMETOLIVE = "blur.shard.cache.max.timetolive";
  public static final String BLUR_SHARD_FILTER_CACHE_CLASS = "blur.shard.filter.cache.class";
  public static final String BLUR_INDEXMANAGER_SEARCH_THREAD_COUNT = "blur.indexmanager.search.thread.count";
  public static final String BLUR_INDEXMANAGER_MUTATE_THREAD_COUNT = "blur.indexmanager.mutate.thread.count";
  public static final String BLUR_INDEXMANAGER_FACET_THREAD_COUNT = "blur.indexmanager.facet.thread.count";
  public static final String BLUR_SHARD_DATA_FETCH_THREAD_COUNT = "blur.shard.data.fetch.thread.count";
  public static final String BLUR_MAX_CLAUSE_COUNT = "blur.max.clause.count";
  public static final String BLUR_SHARD_CACHE_MAX_QUERYCACHE_ELEMENTS = "blur.shard.cache.max.querycache.elements";
  public static final String BLUR_SHARD_OPENER_THREAD_COUNT = "blur.shard.opener.thread.count";
  public static final String BLUR_SHARD_MERGE_THREAD_COUNT = "blur.shard.merge.thread.count";
  public static final String BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE = "blur.shard.index.deletion.policy.maxage";
  public static final String BLUR_SHARD_INDEX_SIMILARITY = "blur.shard.index.similarity";
  public static final String BLUR_SHARD_THRIFT_SELECTOR_THREADS = "blur.shard.thrift.selector.threads";
  public static final String BLUR_SHARD_THRIFT_MAX_READ_BUFFER_BYTES = "blur.shard.thrift.max.read.buffer.bytes";
  public static final String BLUR_SHARD_THRIFT_ACCEPT_QUEUE_SIZE_PER_THREAD = "blur.shard.thrift.accept.queue.size.per.thread";
  public static final String BLUR_SHARD_DEEP_PAGING_CACHE_SIZE = "blur.shard.deep.paging.cache.size";

  public static final String BLUR_SHARD_BLOCK_CACHE_V2_POOL_CACHE_SIZE = "blur.shard.block.cache.v2.pool.cache.size";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_SLAB_CHUNK_SIZE = "blur.shard.block.cache.v2.slab.chunk.size";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_SLAB_SIZE = "blur.shard.block.cache.v2.slab.size";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_SLAB_ENABLED = "blur.shard.block.cache.v2.slab.enabled";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT = "blur.shard.block.cache.v2.read.cache.ext";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT = "blur.shard.block.cache.v2.read.nocache.ext";
  public static final String DEFAULT_VALUE = "";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT = "blur.shard.block.cache.v2.write.cache.ext";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT = "blur.shard.block.cache.v2.write.nocache.ext";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT = "blur.shard.block.cache.v2.write.default";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT = "blur.shard.block.cache.v2.read.default";
  public static final String OFF_HEAP = "OFF_HEAP";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_STORE = "blur.shard.block.cache.v2.store";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX = "blur.shard.block.cache.v2.cacheBlockSize.";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_DIRECT_REF_LIMIT_PREFIX = "blur.shard.block.cache.v2.directRefLimit.";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE = "blur.shard.block.cache.v2.fileBufferSize";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE = "blur.shard.block.cache.v2.cacheBlockSize";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_DIRECT_REF_LIMIT = "blur.shard.block.cache.v2.directRefLimit";
  public static final String BLUR_SHARD_BLURINDEX_CLASS = "blur.shard.blurindex.class";
  public static final String BLUR_SHARD_SERVER_MINIMUM_BEFORE_SAFEMODE_EXIT = "blur.shard.server.minimum.before.safemode.exit";
  public static final String BLUR_SHARD_SMALL_MERGE_THRESHOLD = "blur.shard.small.merge.threshold";
  public static final String BLUR_SHARD_REQUEST_CACHE_SIZE = "blur.shard.request.cache.size";
  public static final String BLUR_GC_BACK_PRESSURE_HEAP_RATIO = "blur.gc.back.pressure.heap.ratio";
  public static final String BLUR_SHARD_BLOCK_CACHE_V2_QUIET_MERGES = "blur.shard.block.cache.v2.quiet.merges";

  public static final String BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_SKIP_THRESHOLD = "blur.shard.default.read.sequential.skip.threshold";
  public static final String BLUR_SHARD_DEFAULT_READ_SEQUENTIAL_THRESHOLD = "blur.shard.default.read.sequential.threshold";
  public static final String BLUR_SHARD_MERGE_READ_SEQUENTIAL_SKIP_THRESHOLD = "blur.shard.merge.read.sequential.skip.threshold";
  public static final String BLUR_SHARD_MERGE_READ_SEQUENTIAL_THRESHOLD = "blur.shard.merge.read.sequential.threshold";

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
  public static final String BLUR_CLIENTPOOL_CLIENT_MAX_CONNECTIONS_PER_HOST = "blur.clientpool.client.max.connections.per.host";
  public static final String BLUR_CLIENTPOOL_CLIENT_STALE_THRESHOLD = "blur.clientpool.client.stale.threshold";
  public static final String BLUR_CLIENTPOOL_CLIENT_CLEAN_FREQUENCY = "blur.clientpool.client.clean.frequency";
  public static final String BLUR_LUCENE_FST_BYTEARRAY_FACTORY = "blur.lucene.fst.bytearray.factory";

  public static final String BLUR_THRIFT_MAX_FRAME_SIZE = "blur.thrift.max.frame.size";
  public static final int BLUR_THRIFT_DEFAULT_MAX_FRAME_SIZE = 16384000;

  public static final String BLUR_SHARD_FILTERED_SERVER_CLASS = "blur.shard.filtered.server.class";
  public static final String BLUR_CONTROLLER_FILTERED_SERVER_CLASS = "blur.controller.filtered.server.class";

  public static final String BLUR_GUI_CONTROLLER_PORT = "blur.gui.controller.port";
  public static final String BLUR_GUI_SHARD_PORT = "blur.gui.shard.port";

  public static final String DEFAULT = "default";
  public static final String BLUR_CLUSTER_NAME = "blur.cluster.name";
  public static final String BLUR_CLUSTER;
  public static final String BLUR_HTTP_STATUS_RUNNING_PORT = "blur.http.status.running.port";
  public static final String BLUR_STREAM_SERVER_RUNNING_PORT = "blur.stream.server.running.port";
  public static final String BLUR_STREAM_SERVER_THREADS = "blur.stream.server.threads";

  public static final String BLUR_SHARD_COMMAND_DRIVER_THREADS = "blur.shard.command.driver.threads";
  public static final String BLUR_SHARD_COMMAND_WORKER_THREADS = "blur.shard.command.worker.threads";
  public static final String BLUR_CONTROLLER_COMMAND_DRIVER_THREADS = "blur.controller.command.driver.threads";
  public static final String BLUR_CONTROLLER_COMMAND_WORKER_THREADS = "blur.controller.command.worker.threads";
  public static final String BLUR_COMMAND_LIB_PATH = "blur.command.lib.path";
  public static final String BLUR_TMP_PATH = "blur.tmp.path";
  public static final String BLUR_NODENAME = "blur.nodename";

  public static final String BLUR_SECURITY_SASL_TYPE = "blur.security.sasl.type";
  public static final String BLUR_SECURITY_SASL_ENABLED = "blur.security.sasl.enabled";
  public static final String BLUR_SECUTIRY_SASL_CUSTOM_CLASS = "blur.security.sasl.CUSTOM.class";

  public static final String BLUR_SECURITY_SASL_LDAP_DOMAIN = "blur.security.sasl.LDAP.domain";
  public static final String BLUR_SECURITY_SASL_LDAP_BASEDN = "blur.security.sasl.LDAP.basedn";
  public static final String BLUR_SECURITY_SASL_LDAP_URL = "blur.security.sasl.LDAP.url";

  public static final String BLUR_SERVER_SECURITY_FILTER_CLASS = "blur.server.security.filter.class.";

  public static final String BLUR_HOME = "BLUR_HOME";

  public static final long ZK_WAIT_TIME = TimeUnit.SECONDS.toMillis(5);
  public static final String DELETE_MARKER_VALUE = "delete";
  public static final String DELETE_MARKER = "_deletemarker_";

  public static final String SHARED_MERGE_SCHEDULER_PREFIX = "shared-merge-scheduler";

  public static final String BLUR_FILTER_ALIAS = "blur.filter.alias.";
  
  public static final String HADOOP_CONF = "hadoop_conf.";

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
