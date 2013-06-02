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
import java.util.List;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.server.ShardContext;
import org.apache.blur.server.TableContext;
import org.apache.blur.thrift.generated.ColumnPreCache;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;

public class FieldBasedWarmer extends IndexReaderWarmer {

  private static final Log LOG = LogFactory.getLog(FieldBasedWarmer.class);

  private List<String> preCacheCols;

  public FieldBasedWarmer(ShardContext shardContext) {
    this(shardContext.getTableContext());
  }

  public FieldBasedWarmer(TableContext tableContext) {
    this(tableContext.getDescriptor());
  }

  public FieldBasedWarmer(TableDescriptor tableDescriptor) {
    this(tableDescriptor.getColumnPreCache());
  }

  public FieldBasedWarmer(ColumnPreCache columnPreCache) {
    this(columnPreCache == null ? null : columnPreCache.getPreCacheCols());
  }

  public FieldBasedWarmer(List<String> preCacheCols) {
    this.preCacheCols = preCacheCols;
  }

  @Override
  public void warm(AtomicReader reader) throws IOException {
    if (preCacheCols != null) {
      warm(reader, preCacheCols);
    } else {
      Fields fields = reader.fields();
      warm(reader, fields);
    }
  }

  private void warm(AtomicReader reader, Iterable<String> fieldNames) throws IOException {
    // for (String field : fieldNames) {
    // LOG.debug("Warming field [{0}] in reader [{1}]", field, reader);
    //
    // AtomicReaderContext context = reader.getContext();
    // context.
    //
    // Fields fields = reader.fields();
    // Terms terms = fields.terms(field);
    // TermsEnum termsEnum = terms.iterator(null);
    // BytesRef ref = null;
    // Term term = new Term(field);
    // while ((ref = termsEnum.next()) != null) {
    // term.set(field, ref);
    // DocsAndPositionsEnum termPositionsEnum = reader.termPositionsEnum(term);
    // if (termPositionsEnum != null) {
    // while (termPositionsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
    // int freq = termPositionsEnum.freq();
    // for (int i = 0; i < freq; i++) {
    // termPositionsEnum.nextPosition();
    // }
    // }
    // }
    // }
    // }
  }
}
