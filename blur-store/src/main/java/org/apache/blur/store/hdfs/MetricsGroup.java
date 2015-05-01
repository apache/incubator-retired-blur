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

package org.apache.blur.store.hdfs;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

public class MetricsGroup {
  final Histogram readRandomAccess;
  final Histogram readStreamAccess;
  final Histogram writeAccess;
  final Meter readRandomThroughput;
  final Meter readStreamThroughput;
  final Meter readStreamSeek;
  final Meter writeThroughput;
  final Counter totalHdfsBlock;
  final Counter localHdfsBlock;

  MetricsGroup(Histogram readRandomAccess, Histogram readStreamAccess, Histogram writeAccess,
      Meter readRandomThroughput, Meter readStreamThroughput, Meter readStreamSeek, Meter writeThroughput,
      Counter totalHdfsBlock, Counter localHdfsBlock) {
    this.readRandomAccess = readRandomAccess;
    this.readStreamAccess = readStreamAccess;
    this.writeAccess = writeAccess;
    this.readRandomThroughput = readRandomThroughput;
    this.readStreamThroughput = readStreamThroughput;
    this.writeThroughput = writeThroughput;
    this.readStreamSeek = readStreamSeek;
    this.totalHdfsBlock = totalHdfsBlock;
    this.localHdfsBlock = localHdfsBlock;
  }
}