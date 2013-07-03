package org.apache.blur.server;

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
import static org.apache.blur.utils.BlurConstants.BLUR_SAHRD_INDEX_SIMILARITY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_COMMITS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_REFRESHS;
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.analysis.BaseFieldManager;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;

public class TableContext {

  private static final Log LOG = LogFactory.getLog(TableContext.class);

  private static final String LOGS = "logs";

  private Path tablePath;
  private Path walTablePath;
  private String defaultFieldName;
  private String table;
  private IndexDeletionPolicy indexDeletionPolicy;
  private Similarity similarity;
  private Configuration configuration;
  private TableDescriptor descriptor;
  private long timeBetweenCommits;
  private long timeBetweenRefreshs;
  private ScoreType defaultScoreType;
  private Term defaultPrimeDocTerm;
  private FieldManager fieldManager;

  private static ConcurrentHashMap<String, TableContext> cache = new ConcurrentHashMap<String, TableContext>();

  protected TableContext() {

  }

  public static void clear() {
    cache.clear();
  }

  public static TableContext create(TableDescriptor tableDescriptor) {
    TableContext tableContext = cache.get(tableDescriptor.getName());
    if (tableContext != null) {
      return tableContext;
    }
    LOG.info("Creating table context for table [{0}]", tableDescriptor.getName());
    Configuration configuration = new Configuration();
    Map<String, String> tableProperties = tableDescriptor.getTableProperties();
    if (tableProperties != null) {
      for (Entry<String, String> prop : tableProperties.entrySet()) {
        configuration.set(prop.getKey(), prop.getValue());
      }
    }

    tableContext = new TableContext();
    tableContext.configuration = configuration;
    tableContext.tablePath = new Path(tableDescriptor.getTableUri());
    tableContext.walTablePath = new Path(tableContext.tablePath, LOGS);

    tableContext.defaultFieldName = SUPER;
    tableContext.table = tableDescriptor.getName();
    tableContext.descriptor = tableDescriptor;
    tableContext.timeBetweenCommits = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_COMMITS, 60000);
    tableContext.timeBetweenRefreshs = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_REFRESHS, 5000);
    tableContext.defaultPrimeDocTerm = new Term("_prime_", "true");
    tableContext.defaultScoreType = ScoreType.SUPER;

    tableContext.fieldManager = null;

    Class<?> c1 = configuration.getClass(BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE,
        KeepOnlyLastCommitDeletionPolicy.class);
    tableContext.indexDeletionPolicy = (IndexDeletionPolicy) configure(ReflectionUtils.newInstance(c1, configuration),
        tableContext);
    Class<?> c2 = configuration.getClass(BLUR_SAHRD_INDEX_SIMILARITY, DefaultSimilarity.class);
    tableContext.similarity = (Similarity) configure(ReflectionUtils.newInstance(c2, configuration), tableContext);

    cache.put(tableDescriptor.getName(), tableContext);
    return tableContext;
  }

  private static Object configure(Object o, TableContext tableContext) {
    if (o instanceof Configurable) {
      ((Configurable) o).setTableContext(tableContext);
    }
    return o;
  }

  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return indexDeletionPolicy;
  }

  public Similarity getSimilarity() {
    return similarity;
  }

  public long getTimeBetweenCommits() {
    return timeBetweenCommits;
  }

  public long getTimeBetweenRefreshs() {
    return timeBetweenRefreshs;
  }

  public FieldManager getFieldManager() {
    return fieldManager;
  }

  public String getTable() {
    return table;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public TableDescriptor getDescriptor() {
    return descriptor;
  }

  public Path getTablePath() {
    return tablePath;
  }

  public Path getWalTablePath() {
    return walTablePath;
  }

  public String getDefaultFieldName() {
    return defaultFieldName;
  }

  public Term getDefaultPrimeDocTerm() {
    return defaultPrimeDocTerm;
  }

  public ScoreType getDefaultScoreType() {
    return defaultScoreType;
  }

  public long getTimeBetweenWALSyncsNanos() {
    return TimeUnit.MILLISECONDS.toNanos(10);
  }
}
