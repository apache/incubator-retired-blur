package org.apache.blur.analysis;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;

public class HdfsFieldManager extends BaseFieldManager {

  public static abstract class Lock {

    public abstract void lock();

    public abstract void unlock();

  }

  private static final Log LOG = LogFactory.getLog(HdfsFieldManager.class);

  private static final String FIELD_TYPE = "_fieldType_";
  private static final String FIELD_LESS_INDEXING = "_fieldLessIndexing_";
  private static final String FAMILY = "_family_";
  private static final String COLUMN_NAME = "_columnName_";
  private static final String SUB_COLUMN_NAME = "_subColumnName_";

  private static Lock _lock = new Lock() {
    private final java.util.concurrent.locks.Lock _javalock = new ReentrantReadWriteLock().writeLock();

    @Override
    public void lock() {
      _javalock.lock();
    }

    @Override
    public void unlock() {
      _javalock.unlock();
    }
  };

  private final Configuration _configuration;
  private final Path _storagePath;
  private final FileSystem _fileSystem;

  public HdfsFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, Path storagePath,
      Configuration configuration) throws IOException {
    this(fieldLessField, defaultAnalyzerForQuerying, storagePath, configuration, true, null, false, null);
  }

  public HdfsFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, Path storagePath,
      Configuration configuration, boolean strict, String defaultMissingFieldType,
      boolean defaultMissingFieldLessIndexing, Map<String, String> defaultMissingFieldProps) throws IOException {
    super(fieldLessField, defaultAnalyzerForQuerying, strict, defaultMissingFieldType, defaultMissingFieldLessIndexing,
        defaultMissingFieldProps);
    _storagePath = storagePath;
    _configuration = configuration;
    _fileSystem = _storagePath.getFileSystem(_configuration);
  }

  @Override
  protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) throws IOException {
    // Might want to make this a ZK lock
    _lock.lock();
    try {
      String fieldType = fieldTypeDefinition.getFieldType();
      boolean fieldLessIndexed = fieldTypeDefinition.isFieldLessIndexed();
      LOG.info("Attempting to store new field [{0}] with fieldLessIndexing [{1}] with type [{2}] and properties [{3}]",
          fieldName, fieldLessIndexed, fieldType, fieldTypeDefinition.getProperties());
      Properties properties = new Properties();
      setProperty(properties, FAMILY, fieldTypeDefinition.getFamily());
      setProperty(properties, FAMILY, fieldTypeDefinition.getFamily());
      setProperty(properties, COLUMN_NAME, fieldTypeDefinition.getColumnName());
      setProperty(properties, SUB_COLUMN_NAME, fieldTypeDefinition.getSubColumnName());
      setProperty(properties, FIELD_LESS_INDEXING, Boolean.toString(fieldLessIndexed));
      setProperty(properties, FIELD_TYPE, fieldType);
      Map<String, String> props = fieldTypeDefinition.getProperties();
      if (props != null) {
        for (Entry<String, String> e : props.entrySet()) {
          properties.setProperty(e.getKey(), e.getValue());
        }
      }
      Path path = getFieldPath(fieldName);
      if (_fileSystem.exists(path)) {
        LOG.info("Field [{0}] already exists.", fieldName);
        return false;
      }
      Path tmpPath = new Path(path.getParent(), UUID.randomUUID().toString() + ".tmp");
      FSDataOutputStream outputStream = _fileSystem.create(tmpPath, false);
      properties.store(outputStream, getComments());
      outputStream.close();
      if (_fileSystem.rename(tmpPath, path)) {
        // @TODO make this configurable
        _fileSystem.setReplication(path, (short) 10);
        return true;
      } else {
        _fileSystem.delete(tmpPath, false);
        LOG.info("Field [{0}] already exists.", fieldName, fieldLessIndexed, fieldType, props);
        return false;
      }
    } finally {
      _lock.unlock();
    }
  }

  private void setProperty(Properties properties, String key, String value) {
    if (value == null) {
      return;
    }
    properties.setProperty(key, value);
  }

  private Path getFieldPath(String fieldName) {
    return new Path(_storagePath, fieldName);
  }

  private String getComments() {
    return "This file is generated from Apache Blur to store meta data about field types.  DO NOT MODIFIY!";
  }

  @Override
  protected void tryToLoad(String fieldName) throws IOException {
    _lock.lock();
    try {
      Path path = getFieldPath(fieldName);
      if (!_fileSystem.exists(path)) {
        return;
      }
      FSDataInputStream inputStream = _fileSystem.open(path);
      Properties properties = new Properties();
      properties.load(inputStream);
      inputStream.close();
      boolean fieldLessIndexing = Boolean.parseBoolean(properties.getProperty(FIELD_LESS_INDEXING));
      String fieldType = properties.getProperty(FIELD_TYPE);
      Map<String, String> props = toMap(properties);
      FieldTypeDefinition fieldTypeDefinition = newFieldTypeDefinition(fieldName, fieldLessIndexing, fieldType, props);
      fieldTypeDefinition.setFamily(properties.getProperty(FAMILY));
      fieldTypeDefinition.setColumnName(properties.getProperty(COLUMN_NAME));
      fieldTypeDefinition.setSubColumnName(properties.getProperty(SUB_COLUMN_NAME));
      fieldTypeDefinition.setFieldLessIndexed(fieldLessIndexing);
      fieldTypeDefinition.setFieldType(properties.getProperty(FIELD_TYPE));
      fieldTypeDefinition.setProperties(props);
      registerFieldTypeDefinition(fieldName, fieldTypeDefinition);
    } finally {
      _lock.unlock();
    }
  }

  private Map<String, String> toMap(Properties props) {
    Map<String, String> result = new HashMap<String, String>();
    for (Entry<Object, Object> e : props.entrySet()) {
      result.put(e.getKey().toString(), e.getValue().toString());
    }
    result.remove(FAMILY);
    result.remove(COLUMN_NAME);
    result.remove(SUB_COLUMN_NAME);
    result.remove(FIELD_TYPE);
    result.remove(FIELD_LESS_INDEXING);
    return result;
  }

  public static Lock getLock() {
    return _lock;
  }

  public static void setLock(Lock lock) {
    _lock = lock;
  }
}
