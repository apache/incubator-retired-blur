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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;

public class HdfsFieldManager extends BaseFieldManager {

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
  protected boolean tryToStore(String fieldName, boolean fieldLessIndexing, String fieldType, Map<String, String> props) {
    Properties properties = new Properties();
    properties.put("fieldLessIndexing", fieldLessIndexing);
    properties.put("fieldType", fieldType);
    if (props != null) {
      for (Entry<String, String> e : props.entrySet()) {
        properties.put(e.getKey(), e.getValue());
      }
    }
    Path path = new Path(_storagePath, fieldName);
    try {
      if (_fileSystem.exists(path)) {
        return false;
      }
    } catch (IOException e) {
      LOG.error("Could not check filesystem for existence of path [{0}]", e, path);
      return false;
    }
    try {
      FSDataOutputStream outputStream = _fileSystem.create(path, false);
      properties.store(outputStream, getComments());
      outputStream.close();
    } catch (IOException e) {
      LOG.error("Could not create field [" + fieldName + "]", e);
      return false;
    }
    return true;
  }

  private String getComments() {
    return null;
  }

  @Override
  protected void tryToLoad(String field, Map<String, FieldTypeDefinition> fieldNameToDefMap,
      Map<String, Set<String>> columnToSubColumn) {
    
  }

}
