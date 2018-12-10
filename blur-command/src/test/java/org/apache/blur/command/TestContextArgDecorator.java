package org.apache.blur.command;

import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.lucene.search.IndexSearcherCloseable;
import org.apache.blur.server.TableContext;
import org.apache.lucene.index.IndexReader;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestContextArgDecorator extends IndexContext {
  private IndexContext heavyContext;

  public TestContextArgDecorator(IndexContext heavyContext) {
    super();
    this.heavyContext = heavyContext;
  }

  public TableContext getTableContext() throws IOException {
    return heavyContext.getTableContext();
  }

  public Shard getShard() {
    return heavyContext.getShard();
  }

  public IndexReader getIndexReader() {
    return heavyContext.getIndexReader();
  }

  public IndexSearcherCloseable getIndexSearcher() {
    return heavyContext.getIndexSearcher();
  }

  public BlurConfiguration getBlurConfiguration() throws IOException {
    return heavyContext.getBlurConfiguration();
  }

  @Override
  public TableContext getTableContext(String table) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public BlurConfiguration getBlurConfiguration(String table) throws IOException {
    throw new RuntimeException("Not implemented.");
  }

}
