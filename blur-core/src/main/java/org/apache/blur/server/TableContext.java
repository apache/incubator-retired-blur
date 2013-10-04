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
import static org.apache.blur.utils.BlurConstants.BLUR_FIELDTYPE;
import static org.apache.blur.utils.BlurConstants.BLUR_SAHRD_INDEX_SIMILARITY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_COMMITS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_REFRESHS;
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.HdfsFieldManager;
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
  private static final String TYPES = "types";

  private static ConcurrentHashMap<String, TableContext> cache = new ConcurrentHashMap<String, TableContext>();
  private static Configuration systemConfiguration;
  private static BlurConfiguration systemBlurConfiguration;

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
  private BlurConfiguration blurConfiguration;

  protected TableContext() {

  }

  public static void clear() {
    cache.clear();
  }

  @SuppressWarnings("unchecked")
  public static TableContext create(TableDescriptor tableDescriptor) {
    if (tableDescriptor == null) {
      throw new NullPointerException("TableDescriptor can not be null.");
    }
    String name = tableDescriptor.getName();
    if (name == null) {
      throw new NullPointerException("Table name in the TableDescriptor can not be null.");
    }
    String tableUri = tableDescriptor.getTableUri();
    if (tableUri == null) {
      throw new NullPointerException("Table uri in the TableDescriptor can not be null.");
    }
    TableContext tableContext = cache.get(name);
    if (tableContext != null) {
      return tableContext;
    }
    LOG.info("Creating table context for table [{0}]", name);
    Configuration configuration = getSystemConfiguration();
    BlurConfiguration blurConfiguration = getSystemBlurConfiguration();
    Map<String, String> tableProperties = tableDescriptor.getTableProperties();
    if (tableProperties != null) {
      for (Entry<String, String> prop : tableProperties.entrySet()) {
        configuration.set(prop.getKey(), prop.getValue());
        blurConfiguration.set(prop.getKey(), prop.getValue());
      }
    }

    tableContext = new TableContext();
    tableContext.configuration = configuration;
    tableContext.blurConfiguration = blurConfiguration;
    tableContext.tablePath = new Path(tableUri);
    tableContext.walTablePath = new Path(tableContext.tablePath, LOGS);

    tableContext.defaultFieldName = SUPER;
    tableContext.table = name;
    tableContext.descriptor = tableDescriptor;
    tableContext.timeBetweenCommits = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_COMMITS, 60000);
    tableContext.timeBetweenRefreshs = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_REFRESHS, 5000);
    tableContext.defaultPrimeDocTerm = new Term("_prime_", "true");
    tableContext.defaultScoreType = ScoreType.SUPER;

    boolean strict = tableDescriptor.isStrictTypes();
    String defaultMissingFieldType = tableDescriptor.getDefaultMissingFieldType();
    boolean defaultMissingFieldLessIndexing = tableDescriptor.isDefaultMissingFieldLessIndexing();
    Map<String, String> defaultMissingFieldProps = emptyIfNull(tableDescriptor.getDefaultMissingFieldProps());

    Path storagePath = new Path(tableContext.tablePath, TYPES);
    try {
      HdfsFieldManager hdfsFieldManager = new HdfsFieldManager(SUPER, new NoStopWordStandardAnalyzer(), storagePath,
          configuration, strict, defaultMissingFieldType, defaultMissingFieldLessIndexing, defaultMissingFieldProps);
      hdfsFieldManager.load();
      tableContext.fieldManager = hdfsFieldManager;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Set<Entry<String, String>> entrySet = blurConfiguration.getProperties().entrySet();
    for (Entry<String, String> entry : entrySet) {
      String key = entry.getKey();
      if (key.startsWith(BLUR_FIELDTYPE)) {
        String className = entry.getValue();
        LOG.info("Attempting to load new type [{0}]", className);
        Class<? extends FieldTypeDefinition> clazz;
        try {
          clazz = (Class<? extends FieldTypeDefinition>) Class.forName(className);
          FieldTypeDefinition fieldTypeDefinition = clazz.newInstance();
          tableContext.fieldManager.registerType(clazz);
          LOG.info("Sucessfully loaded new type [{0}] with name [{1}]", className, fieldTypeDefinition.getName());
        } catch (ClassNotFoundException e) {
          LOG.error("The field type definition class [{0}] was not found.  Check the classpath.", e, className);
        } catch (InstantiationException e) {
          LOG.error("Could not create the field type definition [{0}].", e, className);
        } catch (IllegalAccessException e) {
          LOG.error("Unknown exception while trying to load field type definition [{0}].", e, className);
        }
      }
    }

    Class<?> c1 = configuration.getClass(BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE,
        KeepOnlyLastCommitDeletionPolicy.class);
    tableContext.indexDeletionPolicy = (IndexDeletionPolicy) configure(ReflectionUtils.newInstance(c1, configuration),
        tableContext);
    Class<?> c2 = configuration.getClass(BLUR_SAHRD_INDEX_SIMILARITY, DefaultSimilarity.class);
    tableContext.similarity = (Similarity) configure(ReflectionUtils.newInstance(c2, configuration), tableContext);

    cache.put(name, tableContext);
    return tableContext;
  }

  private static Map<String, String> emptyIfNull(Map<String, String> defaultMissingFieldProps) {
    if (defaultMissingFieldProps == null) {
      return new HashMap<String, String>();
    }
    return defaultMissingFieldProps;
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

  public BlurConfiguration getBlurConfiguration() {
    return blurConfiguration;
  }

  public static synchronized Configuration getSystemConfiguration() {
    if (systemConfiguration == null) {
      systemConfiguration = new Configuration();
    }
    return systemConfiguration;
  }

  public static void setSystemConfiguration(Configuration systemConfiguration) {
    TableContext.systemConfiguration = systemConfiguration;
  }

  public static synchronized BlurConfiguration getSystemBlurConfiguration() {
    if (systemBlurConfiguration == null) {
      try {
        systemBlurConfiguration = new BlurConfiguration();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return systemBlurConfiguration;
  }

  public static void setSystemBlurConfiguration(BlurConfiguration systemBlurConfiguration) {
    TableContext.systemBlurConfiguration = systemBlurConfiguration;
  }
}
