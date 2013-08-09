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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperFieldManager extends BaseFieldManager {

  private static final String FIELD_TYPE = "fieldType";
  private static final String FIELD_LESS_INDEXING = "fieldLessIndexing";
  private static final Object lock = new Object();

  private static final Log LOG = LogFactory.getLog(ZooKeeperFieldManager.class);
  private final ZooKeeper _zooKeeper;
  private final String _basePath;

  public ZooKeeperFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, ZooKeeper zooKeeper, String basePath)
      throws IOException {
    this(fieldLessField, defaultAnalyzerForQuerying, zooKeeper, basePath, true, null, false, null);
  }

  public ZooKeeperFieldManager(String fieldLessField, Analyzer defaultAnalyzerForQuerying, ZooKeeper zooKeeper,
      String basePath, boolean strict, String defaultMissingFieldType, boolean defaultMissingFieldLessIndexing,
      Map<String, String> defaultMissingFieldProps) throws IOException {
    super(fieldLessField, defaultAnalyzerForQuerying, strict, defaultMissingFieldType, defaultMissingFieldLessIndexing,
        defaultMissingFieldProps);
    _zooKeeper = zooKeeper;
    _basePath = basePath;
  }

  @Override
  protected boolean tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType, Map<String, String> props)
      throws IOException {
    // Might want to make this a ZK lock
    synchronized (lock) {
      LOG.info("Attempting to store new field [{0}] with fieldLessIndexing [{1}] with type [{2}] and properties [{3}]",
          fieldName, fieldLessIndexing, fieldType, props);
      Properties properties = new Properties();
      properties.setProperty(FIELD_LESS_INDEXING, Boolean.toString(fieldLessIndexing));
      properties.setProperty(FIELD_TYPE, fieldType);
      if (props != null) {
        for (Entry<String, String> e : props.entrySet()) {
          properties.setProperty(e.getKey(), e.getValue());
        }
      }
      String path = getFieldPath(fieldName);
      Stat stat;
      try {
        stat = _zooKeeper.exists(path, false);
      } catch (KeeperException e) {
        throw new IOException(e);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      if (stat != null) {
        LOG.info("Field [{0}] already exists.", fieldName, fieldLessIndexing, fieldType, props);
        return false;
      }
      try {
        _zooKeeper.create(path, toBytes(properties), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return true;
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.NODEEXISTS) {
          LOG.info("Field [{0}] already exists.", fieldName, fieldLessIndexing, fieldType, props);
          return false;
        }
        throw new IOException(e);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  private byte[] toBytes(Properties properties) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    properties.store(outputStream, getComments());
    outputStream.close();
    return outputStream.toByteArray();
  }

  private String getFieldPath(String fieldName) {
    return _basePath + "/" + fieldName;
  }

  private String getComments() {
    return "This file is generated from Apache Blur to store meta data about field types.  DO NOT MODIFIY!";
  }

  @Override
  protected void tryToLoad(String fieldName) throws IOException {
    String path = getFieldPath(fieldName);
    Stat stat;
    try {
      stat = _zooKeeper.exists(path, false);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    if (stat != null) {
      return;
    }
    byte[] data;
    try {
      data = _zooKeeper.getData(path, false, stat);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    Properties props = new Properties();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
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
