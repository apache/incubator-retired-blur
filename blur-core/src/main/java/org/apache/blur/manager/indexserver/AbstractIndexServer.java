package org.apache.blur.manager.indexserver;

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
import java.util.Map;

import org.apache.blur.manager.IndexServer;
import org.apache.blur.manager.writer.BlurIndex;

public abstract class AbstractIndexServer implements IndexServer {

  @Override
  public long getRecordCount(String table) throws IOException {
    long recordCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      recordCount += index.getValue().getRecordCount();
    }
    return recordCount;
  }

  @Override
  public long getRowCount(String table) throws IOException {
    long rowCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      rowCount += index.getValue().getRowCount();
    }
    return rowCount;
  }

  @Override
  public long getSegmentImportInProgressCount(String table) throws IOException {
    long segmentImportInProgressCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      segmentImportInProgressCount += index.getValue().getSegmentImportInProgressCount();
    }
    return segmentImportInProgressCount;
  }

  @Override
  public long getSegmentImportPendingCount(String table) throws IOException {
    long segmentImportPendingCount = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      segmentImportPendingCount += index.getValue().getSegmentImportPendingCount();
    }
    return segmentImportPendingCount;
  }

  @Override
  public long getTableSize(String table) throws IOException {
    long tableSize = 0;
    Map<String, BlurIndex> indexes = getIndexes(table);
    for (Map.Entry<String, BlurIndex> index : indexes.entrySet()) {
      tableSize += index.getValue().getOnDiskSize();
    }
    return tableSize;
  }

}
