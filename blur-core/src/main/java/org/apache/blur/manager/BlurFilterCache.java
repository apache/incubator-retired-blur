package org.apache.blur.manager;

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
import org.apache.blur.BlurConfiguration;
import org.apache.blur.lucene.search.SuperParser;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.thrift.generated.Record;
import org.apache.blur.thrift.generated.Row;
import org.apache.lucene.search.Filter;

/**
 * The {@link BlurFilterCache} class provides the ability to cache pre and post
 * filters on a per table basis. The closing and opening methods should be used
 * as hooks to for when tables are being enabled and disabled.
 */
public abstract class BlurFilterCache {

  protected final BlurConfiguration _configuration;

  public BlurFilterCache(BlurConfiguration configuration) {
    _configuration = configuration;
  }

  /**
   * The fetchPreFilter method fetches the cache pre-filter (or {@link Record}
   * Filter) before attempting to execute the filter provided by the user.
   * 
   * @param table
   *          the table name.
   * @param filterStr
   *          the filter query string, should be used as a key.
   * @return the {@link Filter} to execute or not is missing.
   */
  public abstract Filter fetchPreFilter(String table, String filterStr);

  /**
   * The fetchPostFilter method fetches the cache post-filter (or {@link Row}
   * Filter) before attempting to execute the filter provided by the user.
   * 
   * @param table
   *          the table name.
   * @param filterStr
   *          the filter query string, should be used as a key.
   * @return the {@link Filter} to execute or not is missing.
   */
  public abstract Filter fetchPostFilter(String table, String filterStr);

  /**
   * The storePreFilter method stores the parsed pre {@link Filter} (or
   * {@link Record} Filter) for caching, and should return the {@link Filter} to
   * be executed.
   * 
   * @param table
   *          the table name.
   * @param filterStr
   *          the filter query string, should be used as a key.
   * @return the {@link Filter} that was parsed by the {@link SuperParser}.
   */
  public abstract Filter storePreFilter(String table, String filterStr, Filter filter);

  /**
   * The storePreFilter method stores the parsed post {@link Filter} (or
   * {@link Row} Filter) for caching, and should return the {@link Filter} to be
   * executed.
   * 
   * @param table
   *          the table name.
   * @param filterStr
   *          the filter query string, should be used as a key.
   * @return the {@link Filter} that was parsed by the {@link SuperParser}.
   */
  public abstract Filter storePostFilter(String table, String filterStr, Filter filter);

  /**
   * Notifies the cache that the index is closing on this shard server.
   * 
   * @param table
   *          the table name.
   * @param shard
   *          the shard name.
   * @param index
   *          the {@link BlurIndex}.
   */
  public abstract void closing(String table, String shard, BlurIndex index);

  /**
   * Notifies the cache that the index is opening on this shard server.
   * 
   * @param table
   *          the table name.
   * @param shard
   *          the shard name.
   * @param index
   *          the {@link BlurIndex}.
   */
  public abstract void opening(String table, String shard, BlurIndex index);

}
