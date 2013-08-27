package org.apache.blur.analysis.type;

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
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import org.apache.blur.analysis.FieldTypeDefinition;
import org.apache.blur.analysis.NoStopWordStandardAnalyzer;
import org.apache.blur.lucene.LuceneVersionConstant;
import org.apache.blur.thrift.generated.Column;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class TextFieldTypeDefinition extends FieldTypeDefinition {

  public static final String STOP_WORD_PATH = "stopWordPath";
  public static final String NAME = "text";
  public static final FieldType TYPE_NOT_STORED;
  public static final FieldType TYPE_STORED;

  private Analyzer _analyzer;

  static {
    TYPE_STORED = new FieldType(TextField.TYPE_STORED);
    TYPE_STORED.setOmitNorms(true);
    TYPE_STORED.freeze();

    TYPE_NOT_STORED = new FieldType(TextField.TYPE_NOT_STORED);
    TYPE_NOT_STORED.setOmitNorms(true);
    TYPE_NOT_STORED.freeze();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void configure(String fieldNameForThisInstance, Map<String, String> properties, Configuration configuration) {
    String stopWordUri = properties.get(STOP_WORD_PATH);
    if (stopWordUri == null) {
      _analyzer = new NoStopWordStandardAnalyzer();
    } else {
      try {
        Path path = new Path(stopWordUri);
        FileSystem fileSystem = path.getFileSystem(configuration);
        Reader reader = new InputStreamReader(fileSystem.open(path));
        // Reader closed by analyzer
        _analyzer = new StandardAnalyzer(LuceneVersionConstant.LUCENE_VERSION, reader);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Iterable<? extends Field> getFieldsForColumn(String family, Column column) {
    String name = getName(family, column.getName());
    Field field = new Field(name, column.getValue(), TYPE_STORED);
    return makeIterable(field);
  }

  @Override
  public Iterable<? extends Field> getFieldsForSubColumn(String family, Column column, String subName) {
    String name = getName(family, column.getName(), subName);
    Field field = new Field(name, column.getValue(), TYPE_NOT_STORED);
    return makeIterable(field);
  }

  @Override
  public Analyzer getAnalyzerForIndex(String fieldName) {
    return _analyzer;
  }

  @Override
  public Analyzer getAnalyzerForQuery(String fieldName) {
    return _analyzer;
  }

  @Override
  public boolean checkSupportForFuzzyQuery() {
    return true;
  }

  @Override
  public boolean checkSupportForWildcardQuery() {
    return true;
  }

  @Override
  public boolean checkSupportForPrefixQuery() {
    return true;
  }

  @Override
  public boolean isNumeric() {
    return false;
  }

  @Override
  public boolean checkSupportForCustomQuery() {
    return false;
  }
}
