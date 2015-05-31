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
package org.apache.blur.manager.indexserver;

import static org.apache.blur.metrics.MetricsConstants.BLUR;
import static org.apache.blur.metrics.MetricsConstants.INDEX_COUNT;
import static org.apache.blur.metrics.MetricsConstants.INDEX_MEMORY_USAGE;
import static org.apache.blur.metrics.MetricsConstants.ORG_APACHE_BLUR;
import static org.apache.blur.metrics.MetricsConstants.RECORD_COUNT;
import static org.apache.blur.metrics.MetricsConstants.SEGMENT_COUNT;
import static org.apache.blur.metrics.MetricsConstants.TABLE_COUNT;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.blur.manager.clusterstatus.ClusterStatus;
import org.apache.blur.metrics.AtomicLongGauge;
import org.apache.blur.server.TableContext;
import org.apache.hadoop.conf.Configuration;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;

public abstract class AbstractDistributedIndexServer extends AbstractIndexServer {

  protected final ClusterStatus _clusterStatus;
  protected final Configuration _configuration;
  protected final String _nodeName;
  protected final String _cluster;

  protected final AtomicLong _tableCount = new AtomicLong();
  protected final AtomicLong _indexCount = new AtomicLong();
  protected final AtomicLong _segmentCount = new AtomicLong();
  protected final AtomicLong _indexMemoryUsage = new AtomicLong();
  protected final AtomicLong _recordCount = new AtomicLong();

  public AbstractDistributedIndexServer(ClusterStatus clusterStatus, Configuration configuration, String nodeName,
      String cluster) {
    _clusterStatus = clusterStatus;
    _configuration = configuration;
    _nodeName = nodeName;
    _cluster = cluster;
    MetricName tableCount = new MetricName(ORG_APACHE_BLUR, BLUR, TABLE_COUNT, _cluster);
    MetricName indexCount = new MetricName(ORG_APACHE_BLUR, BLUR, INDEX_COUNT, _cluster);
    MetricName segmentCount = new MetricName(ORG_APACHE_BLUR, BLUR, SEGMENT_COUNT, _cluster);
    MetricName indexMemoryUsage = new MetricName(ORG_APACHE_BLUR, BLUR, INDEX_MEMORY_USAGE, _cluster);
    MetricName recordCount = new MetricName(ORG_APACHE_BLUR, BLUR, RECORD_COUNT, _cluster);

    Metrics.newGauge(tableCount, new AtomicLongGauge(_tableCount));
    Metrics.newGauge(indexCount, new AtomicLongGauge(_indexCount));
    Metrics.newGauge(segmentCount, new AtomicLongGauge(_segmentCount));
    Metrics.newGauge(indexMemoryUsage, new AtomicLongGauge(_indexMemoryUsage));
    Metrics.newGauge(recordCount, new AtomicLongGauge(_recordCount));
  }

  @Override
  public final String getNodeName() {
    return _nodeName;
  }

  protected final TableContext getTableContext(final String table) {
    return TableContext.create(_clusterStatus.getTableDescriptor(true, _clusterStatus.getCluster(true, table), table));
  }

  protected final void checkTable(String table) {
    if (_clusterStatus.exists(true, _cluster, table)) {
      return;
    }
    throw new RuntimeException("Table [" + table + "] does not exist.");
  }
}
