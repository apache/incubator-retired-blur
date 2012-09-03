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
import static org.apache.blur.lucene.LuceneConstant.LUCENE_VERSION;
import static org.apache.blur.utils.BlurConstants.PRIME_DOC;
import static org.apache.blur.utils.BlurConstants.RECORD_ID;
import static org.apache.blur.utils.BlurConstants.ROW_ID;
import static org.apache.blur.utils.BlurConstants.SUPER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.thrift.generated.AlternateColumnDefinition;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;


public class BlurAnalyzer extends Analyzer {

  private static final String STANDARD = "org.apache.lucene.analysis.standard.StandardAnalyzer";

  public static final BlurAnalyzer BLANK_ANALYZER = new BlurAnalyzer(new KeywordAnalyzer());

  private static Map<String, Class<? extends Analyzer>> aliases = new HashMap<String, Class<? extends Analyzer>>();

  private Map<String, Store> _storeMap = new HashMap<String, Store>();
  private Map<String, Set<String>> _subIndexNameLookups = new HashMap<String, Set<String>>();
  private Set<String> _fullTextFields = new HashSet<String>();
  private AnalyzerDefinition _analyzerDefinition;
  private PerFieldAnalyzerWrapper _wrapper;
  private Analyzer _fullTextAnalyzer = new StandardAnalyzer(LUCENE_VERSION);

  private HashMap<String, Analyzer> _analyzers;

  public void addSubField(String name) {
    int lastIndexOf = name.lastIndexOf('.');
    String mainFieldName = name.substring(0, lastIndexOf);
    Set<String> set = _subIndexNameLookups.get(mainFieldName);
    if (set == null) {
      set = new TreeSet<String>();
      _subIndexNameLookups.put(mainFieldName, set);
    }
    set.add(name);
  }

  public Set<String> getSubIndexNames(String indexName) {
    return _subIndexNameLookups.get(indexName);
  }

  public BlurAnalyzer(Analyzer analyzer) {
    _analyzerDefinition = new AnalyzerDefinition();
    _wrapper = new PerFieldAnalyzerWrapper(analyzer);
  }

  public BlurAnalyzer(AnalyzerDefinition analyzerDefinition) {
    _analyzerDefinition = analyzerDefinition;
    ColumnDefinition defaultDefinition = analyzerDefinition.getDefaultDefinition();
    String fullTextAnalyzerClassName = analyzerDefinition.fullTextAnalyzerClassName;
    if (fullTextAnalyzerClassName != null) {
      _fullTextAnalyzer = getAnalyzerByClassName(fullTextAnalyzerClassName, aliases);
    }
    if (defaultDefinition == null) {
      defaultDefinition = new ColumnDefinition(STANDARD, true, null);
      analyzerDefinition.setDefaultDefinition(defaultDefinition);
    }
    Analyzer defaultAnalyzer = getAnalyzerByClassName(defaultDefinition.getAnalyzerClassName(), aliases);
    KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
    _analyzers = new HashMap<String, Analyzer>();
    _analyzers.put(ROW_ID, keywordAnalyzer);
    _analyzers.put(RECORD_ID, keywordAnalyzer);
    _analyzers.put(PRIME_DOC, keywordAnalyzer);
    _analyzers.put(SUPER, _fullTextAnalyzer);
    load(_analyzers);
    _wrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer, _analyzers);
  }

  public FieldConverter getFieldConverter(String name) {
    if (_analyzers == null) {
      return null;
    }
    Analyzer analyzer = _analyzers.get(name);
    if (analyzer != null && analyzer instanceof FieldConverter) {
      return (FieldConverter) analyzer;
    }
    return null;
  }

  private void load(Map<String, Analyzer> analyzers) {
    Map<String, ColumnFamilyDefinition> familyDefinitions = _analyzerDefinition.columnFamilyDefinitions;
    if (familyDefinitions != null) {
      for (String family : familyDefinitions.keySet()) {
        ColumnFamilyDefinition familyDefinition = familyDefinitions.get(family);
        load(family, familyDefinition, analyzers);
      }
    }
  }

  private void load(String family, ColumnFamilyDefinition familyDefinition, Map<String, Analyzer> analyzers) {
    Map<String, ColumnDefinition> columnDefinitions = familyDefinition.columnDefinitions;
    for (String column : columnDefinitions.keySet()) {
      ColumnDefinition columnDefinition = columnDefinitions.get(column);
      load(family, familyDefinition, column, columnDefinition, analyzers);
    }
  }

  private void load(String family, ColumnFamilyDefinition familyDefinition, String column, ColumnDefinition columnDefinition, Map<String, Analyzer> analyzers) {
    Map<String, AlternateColumnDefinition> alternateColumnDefinitions = columnDefinition.alternateColumnDefinitions;
    if (alternateColumnDefinitions != null) {
      for (String subColumn : alternateColumnDefinitions.keySet()) {
        AlternateColumnDefinition alternateColumnDefinition = alternateColumnDefinitions.get(subColumn);
        load(family, familyDefinition, column, columnDefinition, subColumn, alternateColumnDefinition, analyzers);
      }
    }
    String fieldName = family + "." + column;
    Analyzer analyzer = getAnalyzerByClassName(columnDefinition.getAnalyzerClassName(), aliases);
    analyzers.put(fieldName, analyzer);
    if (columnDefinition.isFullTextIndex()) {
      _fullTextFields.add(fieldName);
    }
  }

  private void load(String family, ColumnFamilyDefinition familyDefinition, String column, ColumnDefinition columnDefinition, String subColumn,
      AlternateColumnDefinition alternateColumnDefinition, Map<String, Analyzer> analyzers) {
    String fieldName = family + "." + column + "." + subColumn;
    Analyzer analyzer = getAnalyzerByClassName(alternateColumnDefinition.getAnalyzerClassName(), aliases);
    analyzers.put(fieldName, analyzer);
    putStore(fieldName, Store.NO);
    addSubField(fieldName);
  }

  public void putStore(String name, Store store) {
    _storeMap.put(name, store);
  }

  @SuppressWarnings("unchecked")
  private static Analyzer getAnalyzerByClassName(String className, Map<String, Class<? extends Analyzer>> aliases) {
    if (FieldConverterUtil.isType(className)) {
      return FieldConverterUtil.getAnalyzer(className);
    }
    try {
      Class<? extends Analyzer> clazz = aliases.get(className);
      if (clazz == null) {
        clazz = (Class<? extends Analyzer>) Class.forName(className);
      }
      try {
        return (Analyzer) clazz.newInstance();
      } catch (Exception e) {
        Constructor<?> constructor = clazz.getConstructor(new Class[] { Version.class });
        return (Analyzer) constructor.newInstance(LUCENE_VERSION);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Store getStore(String indexName) {
    Store store = _storeMap.get(indexName);
    if (store == null) {
      return Store.YES;
    }
    return store;
  }

  public Index getIndex(String indexName) {
    return Index.ANALYZED_NO_NORMS;
  }

  public String toJSON() {
    TMemoryBuffer trans = new TMemoryBuffer(1024);
    TJSONProtocol protocol = new TJSONProtocol(trans);
    try {
      _analyzerDefinition.write(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    trans.close();
    byte[] array = trans.getArray();
    return new String(array, 0, trans.length());
  }

  public boolean isFullTextField(String fieldName) {
    ColumnDefinition defaultDefinition = _analyzerDefinition.getDefaultDefinition();
    if (defaultDefinition != null && defaultDefinition.fullTextIndex) {
      return true;
    }
    return _fullTextFields.contains(fieldName);
  }

  public AnalyzerDefinition getAnalyzerDefinition() {
    return _analyzerDefinition;
  }

  public void close() {
    _wrapper.close();
  }

  public int getOffsetGap(Fieldable field) {
    return _wrapper.getOffsetGap(field);
  }

  public int getPositionIncrementGap(String fieldName) {
    return _wrapper.getPositionIncrementGap(fieldName);
  }

  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return _wrapper.reusableTokenStream(fieldName, reader);
  }

  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return _wrapper.tokenStream(fieldName, reader);
  }

  public static BlurAnalyzer create(File file) throws IOException {
    FileInputStream inputStream = new FileInputStream(file);
    try {
      return create(inputStream);
    } finally {
      inputStream.close();
    }
  }

  public static BlurAnalyzer create(InputStream inputStream) throws IOException {
    TMemoryInputTransport trans = new TMemoryInputTransport(getBytes(inputStream));
    TJSONProtocol protocol = new TJSONProtocol(trans);
    AnalyzerDefinition analyzerDefinition = new AnalyzerDefinition();
    try {
      analyzerDefinition.read(protocol);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    trans.close();
    return new BlurAnalyzer(analyzerDefinition);
  }

  private static byte[] getBytes(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int num;
    while ((num = inputStream.read(buffer)) != -1) {
      outputStream.write(buffer, 0, num);
    }
    inputStream.close();
    outputStream.close();
    return outputStream.toByteArray();
  }

  public static BlurAnalyzer create(String jsonStr) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(jsonStr.getBytes());
    try {
      return create(inputStream);
    } finally {
      inputStream.close();
    }
  }

  public static BlurAnalyzer create(Path path) throws IOException {
    FileSystem fileSystem = FileSystem.get(path.toUri(), new Configuration());
    FSDataInputStream inputStream = fileSystem.open(path);
    try {
      return create(inputStream);
    } finally {
      inputStream.close();
    }
  }
}
