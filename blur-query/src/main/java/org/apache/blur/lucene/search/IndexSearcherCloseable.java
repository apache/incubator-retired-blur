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
package org.apache.blur.lucene.search;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.Similarity;

public interface IndexSearcherCloseable extends Closeable {

  IndexReader getIndexReader();

  TopDocs search(Query query, int i) throws IOException;

  Query rewrite(Query query) throws IOException;

  void search(Query query, Collector collector) throws IOException;

  void setSimilarity(Similarity similarity);

}