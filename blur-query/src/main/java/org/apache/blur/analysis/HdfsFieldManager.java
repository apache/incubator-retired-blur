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

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;

public class HdfsFieldManager extends BaseFieldManager {

  private static final String FIELD_TYPE = "fieldType";
  private static final String FIELD_LESS_INDEXING = "fieldLessIndexing";

  private static final Log LOG = LogFactory.getLog(HdfsFieldManager.class);

  private final Configuration _configuration;
  private final Path _storagePath;
  private final FileSystem _fileSystem;

  public HdfsFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, Path storagePath,
      Configuration configuration) throws IOException {
    super(fieldLessField, defaultAnalyzerForQuerying);
    _storagePath = storagePath;
    _configuration = configuration;
    _fileSystem = _storagePath.getFileSystem(_configuration);
  }

  @Override
  protected boolean tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType, Map<String, String> props)
      throws IOException {
    LOG.info("Attempting to store new field [{0}] with fieldLessIndexing [{1}] with type [{2}] and properties [{3}]",
        fieldName, fieldLessIndexing, fieldType, props);
    Properties properties = new Properties();
    properties.setProperty(FIELD_LESS_INDEXING, Boolean.toString(fieldLessIndexing));
    properties.setProperty(FIELD_TYPE, fieldType);
    if (props != null) {
      for (Entry<String, String> e : props.entrySet()) {
        properties.put(e.getKey(), e.getValue());
      }
    }
    Path path = getFieldPath(fieldName);
    if (_fileSystem.exists(path)) {
      LOG.info("Field [{0}] already exists.", fieldName, fieldLessIndexing, fieldType, props);
      return false;
    }
    FSDataOutputStream outputStream = _fileSystem.create(path, false);
    properties.store(outputStream, getComments());
    outputStream.close();
    return true;
  }

  private Path getFieldPath(String fieldName) {
    return new Path(_storagePath, fieldName);
  }

  private String getComments() {
    return "This file is generated from Apache Blur to store meta data about field types.  DO NOT MODIFIY!";
  }

  @Override
  protected void tryToLoad(String fieldName) throws IOException {
    Path path = getFieldPath(fieldName);
    if (!_fileSystem.exists(path)) {
      return;
    }
    FSDataInputStream inputStream = _fileSystem.open(path);
    Properties props = new Properties();
    props.load(inputStream);
    inputStream.close();
    boolean fieldLessIndexing = Boolean.parseBoolean(props.getProperty(FIELD_LESS_INDEXING));
    String fieldType = props.getProperty(FIELD_TYPE);
    FieldTypeDefinition fieldTypeDefinition = newFieldTypeDefinition(fieldLessIndexing, fieldType, toMap(props));
    registerFieldTypeDefinition(fieldName, fieldTypeDefinition);
  }

  private Map<String, String> toMap(Properties props) {
    Map<String, String> result = new HashMap<String, String>();
    for (Entry<Object, Object> e : props.entrySet()) {
      result.put(e.getKey().toString(), e.getValue().toString());
    }
    return result;
  }
}
