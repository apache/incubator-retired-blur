package org.apache.blur.manager.writer;

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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.index.IndexReader;

public abstract class BlurIndex {

  public abstract void replaceRow(boolean waitToBeVisible, boolean wal, Row row) throws IOException;

  public abstract void deleteRow(boolean waitToBeVisible, boolean wal, String rowId) throws IOException;

  public abstract IndexReader getIndexReader() throws IOException;

  public abstract void close() throws IOException;

  public abstract void refresh() throws IOException;

  public abstract AtomicBoolean isClosed();

  public abstract void optimize(int numberOfSegmentsPerShard) throws IOException;


}
