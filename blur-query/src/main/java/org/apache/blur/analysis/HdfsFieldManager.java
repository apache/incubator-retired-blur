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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.blur.analysis.type.MultiValuedNotAllowedException;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.lucene.analysis.Analyzer;

public class HdfsFieldManager extends BaseFieldManager {

  private static final List<String> EMPTY_LIST = Arrays.asList(new String[] {});

  private static final Log LOG = LogFactory.getLog(HdfsFieldManager.class);

  private static final String FIELD_TYPE = "_fieldType_";
  private static final String FIELD_LESS_INDEXING = "_fieldLessIndexing_";
  private static final String SORTENABLED = "_sortEnabled_";
  private static final String MULTI_VALUE_FIELD = "_multiValueField_";
  private static final String FAMILY = "_family_";
  private static final String COLUMN_NAME = "_columnName_";
  private static final String SUB_COLUMN_NAME = "_subColumnName_";
  private static final String TYPE_FILE_EXT = ".type";

  private static final Lock _lock = new ReentrantReadWriteLock().writeLock();

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
        defaultMissingFieldProps, configuration);
    _storagePath = storagePath;
    _configuration = configuration;
    _fileSystem = _storagePath.getFileSystem(_configuration);
  }

  @Override
  protected List<String> getFieldNamesToLoad() throws IOException {
    Tracer trace = Trace.trace("filesystem - getFieldNamesToLoad", Trace.param("storagePath", _storagePath));
    try {
      if (!_fileSystem.exists(_storagePath)) {
        return EMPTY_LIST;
      }
      FileStatus[] listStatus = _fileSystem.listStatus(_storagePath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          if (path.getName().endsWith(TYPE_FILE_EXT)) {
            return true;
          }
          return false;
        }
      });
      if (listStatus == null) {
        return EMPTY_LIST;
      }
      List<String> fieldNames = new ArrayList<String>();
      for (FileStatus fileStatus : listStatus) {
        if (!fileStatus.isDir()) {
          String fileName = fileStatus.getPath().getName();
          fieldNames.add(fileName.substring(0, fileName.lastIndexOf(TYPE_FILE_EXT)));
        }
      }
      return fieldNames;
    } finally {
      trace.done();
    }
  }

  @Override
  protected boolean tryToStore(FieldTypeDefinition fieldTypeDefinition, String fieldName) throws IOException {
    Tracer trace = Trace.trace("filesystem - tryToStore fieldName", Trace.param("fieldName", fieldName),
        Trace.param("storagePath", _storagePath));
    try {
      // Might want to make this a ZK lock
      _lock.lock();
      try {
        String fieldType = fieldTypeDefinition.getFieldType();
        boolean fieldLessIndexed = fieldTypeDefinition.isFieldLessIndexed();
        boolean sortEnable = fieldTypeDefinition.isSortEnable();
        boolean multiValueField = fieldTypeDefinition.isMultiValueField();
        LOG.info(
            "Attempting to store new field [{0}] with fieldLessIndexing [{1}] with type [{2}] and properties [{3}]",
            fieldName, fieldLessIndexed, fieldType, fieldTypeDefinition.getProperties());
        Properties properties = new Properties();
        setProperty(properties, FAMILY, fieldTypeDefinition.getFamily());
        setProperty(properties, FAMILY, fieldTypeDefinition.getFamily());
        setProperty(properties, COLUMN_NAME, fieldTypeDefinition.getColumnName());
        setProperty(properties, SUB_COLUMN_NAME, fieldTypeDefinition.getSubColumnName());
        setProperty(properties, FIELD_LESS_INDEXING, Boolean.toString(fieldLessIndexed));
        setProperty(properties, SORTENABLED, Boolean.toString(sortEnable));
        setProperty(properties, MULTI_VALUE_FIELD, Boolean.toString(multiValueField));

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
    } finally {
      trace.done();
    }
  }

  private void setProperty(Properties properties, String key, String value) {
    if (value == null) {
      return;
    }
    properties.setProperty(key, value);
  }

  private Path getFieldPath(String fieldName) {
    return new Path(_storagePath, fieldName + TYPE_FILE_EXT);
  }

  private String getComments() {
    return "This file is generated from Apache Blur to store meta data about field types.  DO NOT MODIFY!";
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
      boolean sortenabled = Boolean.parseBoolean(properties.getProperty(SORTENABLED));
      String mvfProp = properties.getProperty(MULTI_VALUE_FIELD);
      boolean multiValueField;
      if (mvfProp == null || mvfProp.trim().isEmpty()) {
        multiValueField = true;
      } else {
        multiValueField = Boolean.parseBoolean(mvfProp);
      }
      String fieldType = properties.getProperty(FIELD_TYPE);
      Map<String, String> props = toMap(properties);

      if (mvfProp == null) {
        if (multiValueField && sortenabled) {
          // @TODO hack because we use to not have multivalue in the schema
          LOG.warn("Changing field [{0}] to be NOT multiValueField.", fieldName);
          multiValueField = false;
        }
      }
      FieldTypeDefinition fieldTypeDefinition;
      try {
        fieldTypeDefinition = newFieldTypeDefinition(fieldName, fieldLessIndexing, fieldType, sortenabled,
            multiValueField, props);
      } catch (MultiValuedNotAllowedException e) {
        if (mvfProp == null) {
          multiValueField = false;
          fieldTypeDefinition = newFieldTypeDefinition(fieldName, fieldLessIndexing, fieldType, sortenabled,
              multiValueField, props);
        } else {
          throw e;
        }
      }
      fieldTypeDefinition.setFamily(properties.getProperty(FAMILY));
      fieldTypeDefinition.setColumnName(properties.getProperty(COLUMN_NAME));
      fieldTypeDefinition.setSubColumnName(properties.getProperty(SUB_COLUMN_NAME));
      fieldTypeDefinition.setFieldLessIndexed(fieldLessIndexing);
      fieldTypeDefinition.setFieldType(properties.getProperty(FIELD_TYPE));
      fieldTypeDefinition.setSortEnable(sortenabled);
      fieldTypeDefinition.setMultiValueField(multiValueField);
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
    result.remove(SORTENABLED);
    result.remove(MULTI_VALUE_FIELD);
    return result;
  }

}
