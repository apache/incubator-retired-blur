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
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_BLURINDEX_CLASS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_INDEX_SIMILARITY;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_READ_INTERCEPTOR;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_COMMITS;
import static org.apache.blur.utils.BlurConstants.BLUR_SHARD_TIME_BETWEEN_REFRESHS;
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import lucene.security.index.AccessControlFactory;
import lucene.security.index.FilterAccessControlFactory;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.HdfsFieldManager;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.analysis.ThriftFieldManager;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.search.FairSimilarity;
import org.apache.blur.manager.ReadInterceptor;
import org.apache.blur.manager.writer.BlurIndex;
import org.apache.blur.manager.writer.BlurIndexCloser;
import org.apache.blur.manager.writer.BlurIndexSimpleWriter;
//import org.apache.blur.manager.writer.BlurNRTIndex;
import org.apache.blur.manager.writer.SharedMergeScheduler;
import org.apache.blur.server.cache.ThriftCache;
import org.apache.blur.store.hdfs.HdfsDirectory;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.ScoreType;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;

public class TableContext implements Cloneable {

  private static final Log LOG = LogFactory.getLog(TableContext.class);

  private static final String TYPES = "types";

  private static ConcurrentHashMap<String, TableContext> _cache = new ConcurrentHashMap<String, TableContext>();
  private static Configuration _systemConfiguration;
  private static BlurConfiguration _systemBlurConfiguration;

  private static final ReadInterceptor DEFAULT_INTERCEPTOR = new ReadInterceptor(null) {
    @Override
    public Filter getFilter() {
      return null;
    }
  };

  private Path _tablePath;
  private String _defaultFieldName;
  private String _table;
  private IndexDeletionPolicy _indexDeletionPolicy;
  private Similarity _similarity;
  private Configuration _configuration;
  private TableDescriptor _descriptor;
  private long _timeBetweenCommits;
  private long _timeBetweenRefreshs;
  private ScoreType _defaultScoreType;
  private Term _defaultPrimeDocTerm;
  private FieldManager _fieldManager;
  private BlurConfiguration _blurConfiguration;
  private ReadInterceptor _readInterceptor;
  private AccessControlFactory _accessControlFactory;
  private Set<String> _discoverableFields;

  protected TableContext() {

  }

  public static void clear() {
    _cache.clear();
  }

  public static void clear(String table) {
    _cache.remove(table);
  }

  public static TableContext create(TableDescriptor tableDescriptor) {
    return create(tableDescriptor, false, null);
  }

  public static TableContext create(TableDescriptor tableDescriptor, boolean remote, Iface client) {
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
    TableContext tableContext = _cache.get(name);
    if (tableContext != null) {
      return clone(tableDescriptor, tableContext);
    }
    synchronized (_cache) {
      tableContext = _cache.get(name);
      if (tableContext != null) {
        return clone(tableDescriptor, tableContext);
      }
      return createInternal(tableDescriptor, remote, client, name, tableUri);
    }
  }

  private static TableContext clone(TableDescriptor tableDescriptor, TableContext tableContext) {
    TableContext clone = tableContext.clone();
    TableDescriptor newTd = new TableDescriptor(clone._descriptor);
    clone._descriptor = newTd;
    clone._descriptor.setEnabled(tableDescriptor.isEnabled());
    return clone;
  }

  private static TableContext createInternal(TableDescriptor tableDescriptor, boolean remote, Iface client,
      String name, String tableUri) {
    TableContext tableContext;
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
    tableContext._configuration = configuration;
    tableContext._blurConfiguration = blurConfiguration;
    tableContext._tablePath = new Path(tableUri);

    tableContext._defaultFieldName = SUPER;
    tableContext._table = name;
    tableContext._descriptor = tableDescriptor;
    tableContext._timeBetweenCommits = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_COMMITS, 60000);
    tableContext._timeBetweenRefreshs = configuration.getLong(BLUR_SHARD_TIME_BETWEEN_REFRESHS, 5000);
    tableContext._defaultPrimeDocTerm = new Term(BlurConstants.PRIME_DOC, BlurConstants.PRIME_DOC_VALUE);
    tableContext._defaultScoreType = ScoreType.SUPER;

    // TODO make configurable
    tableContext._discoverableFields = new HashSet<String>(Arrays.asList(BlurConstants.ROW_ID, BlurConstants.RECORD_ID,
        BlurConstants.FAMILY));

    // TODO make configurable
    tableContext._accessControlFactory = new FilterAccessControlFactory();

    boolean strict = tableDescriptor.isStrictTypes();
    String defaultMissingFieldType = tableDescriptor.getDefaultMissingFieldType();
    boolean defaultMissingFieldLessIndexing = tableDescriptor.isDefaultMissingFieldLessIndexing();
    Map<String, String> defaultMissingFieldProps = emptyIfNull(tableDescriptor.getDefaultMissingFieldProps());

    Path storagePath = new Path(tableContext._tablePath, TYPES);
    try {
      FieldManager fieldManager;
      if (remote) {
        fieldManager = new ThriftFieldManager(SUPER, new NoStopWordStandardAnalyzer(), strict, defaultMissingFieldType,
            defaultMissingFieldLessIndexing, defaultMissingFieldProps, configuration, client, name);
      } else {
        fieldManager = new HdfsFieldManager(SUPER, new NoStopWordStandardAnalyzer(), storagePath, configuration,
            strict, defaultMissingFieldType, defaultMissingFieldLessIndexing, defaultMissingFieldProps);
      }
      loadCustomTypes(tableContext, blurConfiguration, fieldManager);
      fieldManager.loadFromStorage();
      tableContext._fieldManager = fieldManager;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Class<?> c1 = configuration.getClass(BLUR_SHARD_INDEX_DELETION_POLICY_MAXAGE,
        KeepOnlyLastCommitDeletionPolicy.class);
    tableContext._indexDeletionPolicy = (IndexDeletionPolicy) configure(ReflectionUtils.newInstance(c1, configuration),
        tableContext);
    Class<?> c2 = configuration.getClass(BLUR_SHARD_INDEX_SIMILARITY, FairSimilarity.class);
    tableContext._similarity = (Similarity) configure(ReflectionUtils.newInstance(c2, configuration), tableContext);

    String readInterceptorClass = blurConfiguration.get(BLUR_SHARD_READ_INTERCEPTOR);
    if (readInterceptorClass == null || readInterceptorClass.trim().isEmpty()) {
      tableContext._readInterceptor = DEFAULT_INTERCEPTOR;
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends ReadInterceptor> clazz = (Class<? extends ReadInterceptor>) Class.forName(readInterceptorClass);
        Constructor<? extends ReadInterceptor> constructor = clazz
            .getConstructor(new Class[] { BlurConfiguration.class });
        tableContext._readInterceptor = constructor.newInstance(blurConfiguration);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    tableContext._similarity = (Similarity) configure(ReflectionUtils.newInstance(c2, configuration), tableContext);
    // DEFAULT_INTERCEPTOR

    _cache.put(name, tableContext);
    return tableContext.clone();
  }

  @SuppressWarnings("unchecked")
  private static void loadCustomTypes(TableContext tableContext, BlurConfiguration blurConfiguration,
      FieldManager fieldManager) {
    Set<Entry<String, String>> entrySet = blurConfiguration.getProperties().entrySet();
    TableDescriptor descriptor = tableContext._descriptor;
    for (Entry<String, String> entry : entrySet) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (value == null || value.isEmpty()) {
        continue;
      }
      if (key.startsWith(BLUR_FIELDTYPE)) {
        String className = entry.getValue();
        descriptor.putToTableProperties(key, className);
        LOG.info("Attempting to load new type [{0}]", className);
        Class<? extends FieldTypeDefinition> clazz;
        try {
          clazz = (Class<? extends FieldTypeDefinition>) Class.forName(className);
          FieldTypeDefinition fieldTypeDefinition = clazz.newInstance();
          fieldManager.registerType(clazz);
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
    return _indexDeletionPolicy;
  }

  public Similarity getSimilarity() {
    return _similarity;
  }

  public long getTimeBetweenCommits() {
    return _timeBetweenCommits;
  }

  public long getTimeBetweenRefreshs() {
    return _timeBetweenRefreshs;
  }

  public FieldManager getFieldManager() {
    return _fieldManager;
  }

  public String getTable() {
    return _table;
  }

  public Configuration getConfiguration() {
    return _configuration;
  }

  public TableDescriptor getDescriptor() {
    return _descriptor;
  }

  public Path getTablePath() {
    return _tablePath;
  }

  public String getDefaultFieldName() {
    return _defaultFieldName;
  }

  public Term getDefaultPrimeDocTerm() {
    return _defaultPrimeDocTerm;
  }

  public ScoreType getDefaultScoreType() {
    return _defaultScoreType;
  }

  public long getTimeBetweenWALSyncsNanos() {
    return TimeUnit.MILLISECONDS.toNanos(10);
  }

  public BlurConfiguration getBlurConfiguration() {
    return _blurConfiguration;
  }

  public static synchronized Configuration getSystemConfiguration() {
    if (_systemConfiguration == null) {
      _systemConfiguration = BlurUtil.newHadoopConfiguration();
    }
    return new Configuration(_systemConfiguration);
  }

  public static void setSystemConfiguration(Configuration systemConfiguration) {
    TableContext._systemConfiguration = systemConfiguration;
  }

  public static synchronized BlurConfiguration getSystemBlurConfiguration() {
    if (_systemBlurConfiguration == null) {
      try {
        _systemBlurConfiguration = new BlurConfiguration();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return _systemBlurConfiguration.clone();
  }

  public static void setSystemBlurConfiguration(BlurConfiguration systemBlurConfiguration) {
    TableContext._systemBlurConfiguration = systemBlurConfiguration;
  }

  @SuppressWarnings("unchecked")
  public BlurIndex newInstanceBlurIndex(ShardContext shardContext, Directory dir, SharedMergeScheduler mergeScheduler,
      ExecutorService searchExecutor, BlurIndexCloser indexCloser, Timer indexImporterTimer, Timer bulkTimer,
      ThriftCache thriftCache) throws IOException {

    String className = _blurConfiguration.get(BLUR_SHARD_BLURINDEX_CLASS, BlurIndexSimpleWriter.class.getName());

    Class<? extends BlurIndex> clazz;
    try {
      clazz = (Class<? extends BlurIndex>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    Constructor<? extends BlurIndex> constructor = findConstructor(clazz);
    try {
      return constructor.newInstance(shardContext, dir, mergeScheduler, searchExecutor, indexCloser,
          indexImporterTimer, bulkTimer, thriftCache);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    } catch (IllegalArgumentException e) {
      throw new IOException(e);
    } catch (InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  private Constructor<? extends BlurIndex> findConstructor(Class<? extends BlurIndex> clazz) throws IOException {
    try {
      return clazz.getConstructor(new Class[] { ShardContext.class, Directory.class, SharedMergeScheduler.class,
          ExecutorService.class, BlurIndexCloser.class, Timer.class, Timer.class, ThriftCache.class });
    } catch (NoSuchMethodException e) {
      throw new IOException(e);
    } catch (SecurityException e) {
      throw new IOException(e);
    }
  }

  public ReadInterceptor getReadInterceptor() {
    return _readInterceptor;
  }

  @Override
  public TableContext clone() {
    try {
      return (TableContext) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public void loadData(String location) throws IOException {
    Path path = new Path(location);
    FileSystem fileSystem = path.getFileSystem(_configuration);

    validateLoad(path, fileSystem);

    FileStatus[] listStatus = fileSystem.listStatus(path);
    for (FileStatus fileStatus : listStatus) {
      loadShard(fileStatus.getPath(), fileSystem);
    }

    // printFS(path, fileSystem);

  }

  private void validateLoad(Path path, FileSystem fileSystem) throws IOException {
    TableDescriptor descriptor = getDescriptor();
    int shardCount = descriptor.getShardCount();
    FileStatus[] listStatus = fileSystem.listStatus(path);
    int count = 0;
    for (FileStatus fileStatus : listStatus) {
      Path shardPath = fileStatus.getPath();
      String shardId = shardPath.getName();
      int shardIndex = ShardUtil.getShardIndex(shardId);
      if (shardIndex >= shardCount) {
        throw new IOException("Too many shards [" + shardIndex + "].");
      }
      count++;
      validateIndexesExist(shardPath, fileSystem);
    }
    if (shardCount != count) {
      throw new IOException("Not enough shards [" + count + "] should be [" + shardCount + "].");
    }
  }

  private void validateIndexesExist(Path shardPath, FileSystem fileSystem) throws IOException {
    FileStatus[] listStatus = fileSystem.listStatus(shardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".commit");
      }
    });
    for (FileStatus fileStatus : listStatus) {
      Path path = fileStatus.getPath();
      HdfsDirectory directory = new HdfsDirectory(_configuration, path);
      try {
        if (!DirectoryReader.indexExists(directory)) {
          throw new IOException("Path [" + path + "] is not a valid index.");
        }
      } finally {
        directory.close();
      }
    }
  }

  private void loadShard(Path newLoadShardPath, FileSystem fileSystem) throws IOException {
    Path tablePath = getTablePath();
    Path shardPath = new Path(tablePath, newLoadShardPath.getName());
    FileStatus[] listStatus = fileSystem.listStatus(newLoadShardPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".commit");
      }
    });

    for (FileStatus fileStatus : listStatus) {
      Path src = fileStatus.getPath();
      Path dst = new Path(shardPath, src.getName());
      if (fileSystem.rename(src, dst)) {
        LOG.info("Successfully moved [{0}] to [{1}].", src, dst);
      } else {
        LOG.info("Could not move [{0}] to [{1}].", src, dst);
        throw new IOException("Could not move [" + src + "] to [" + dst + "].");
      }
    }
  }

  public Set<String> getDiscoverableFields() {
    return _discoverableFields;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }
}
