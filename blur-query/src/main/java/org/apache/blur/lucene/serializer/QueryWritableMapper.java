package org.apache.blur.lucene.serializer;

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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.search.Query;

public class QueryWritableMapper {

  private static Map<Class<? extends Query>, QueryWritableMapper> queryToType = new ConcurrentHashMap<Class<? extends Query>, QueryWritableMapper>();
  private static Map<Integer, QueryWritableMapper> idsToWritable = new ConcurrentHashMap<Integer, QueryWritableMapper>();

  static {
    register(0, new BooleanQueryWritable());
    register(1, new TermQueryWritable());
    register(2, new FuzzyQueryWritable());
    register(3, new MatchAllDocsQueryWritable());
    register(4, new WildcardQueryWritable());
    register(5, new SuperQueryWritable());
  }

  private static synchronized void register(int id, AbtractQueryWritable<? extends Query> queryWritable) {
    QueryWritableMapper qt = new QueryWritableMapper(id, queryWritable);
    idsToWritable.put(id, qt);
    queryToType.put(queryWritable.getType(), qt);
  }

  private final int id;
  private final AbtractQueryWritable<? extends Query> queryWritable;

  private QueryWritableMapper(int id, AbtractQueryWritable<? extends Query> queryWritable) {
    this.id = id;
    this.queryWritable = queryWritable;
  }

  public int getType() {
    return id;
  }

  public AbtractQueryWritable<?> instance() {
    return queryWritable.clone();
  }

  public static QueryWritableMapper lookup(int id) {
    QueryWritableMapper type = idsToWritable.get(id);
    if (type == null) {
      throw new RuntimeException("Type [" + id + "] not found");
    }
    return type;
  }

  public static QueryWritableMapper lookup(Query query) {
    QueryWritableMapper type = queryToType.get(query.getClass());
    if (type == null) {
      throw new RuntimeException("Type [" + query.getClass() + "] for query [" + query + "] not found");
    }
    return type;
  }

  @SuppressWarnings("unchecked")
  public static <R extends Query> AbtractQueryWritable<R> getNewQueryWritable(QueryWritableMapper lookup, Class<R> clazz) {
    QueryWritableMapper type = lookup(lookup.getType());
    return (AbtractQueryWritable<R>) type.instance();
  }
}