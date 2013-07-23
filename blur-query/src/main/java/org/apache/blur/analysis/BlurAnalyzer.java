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
import static org.apache.blur.lucene.LuceneVersionConstant.LUCENE_VERSION;
import static org.apache.blur.utils.BlurConstants.FAMILY;
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
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.protocol.TJSONProtocol;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryBuffer;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TMemoryInputTransport;
import org.apache.blur.thrift.generated.AlternateColumnDefinition;
import org.apache.blur.thrift.generated.AnalyzerDefinition;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.ColumnFamilyDefinition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

public final class BlurAnalyzer extends AnalyzerWrapper {

  public enum TYPE {
    LONG, DOUBLE, FLOAT, INTEGER, TEXT, STRING, STORED
  }

  @SuppressWarnings("serial")
  private static Set<String> typeNameCache = new HashSet<String>() {
    {
      TYPE[] values = TYPE.values();
      for (TYPE t : values) {
        add(t.name());
      }
    }
  };

  private static final String STANDARD = "org.apache.blur.analysis.NoStopWordStandardAnalyzer";
  public static final BlurAnalyzer BLANK_ANALYZER = new BlurAnalyzer(new KeywordAnalyzer());
  private static Map<String, Class<? extends Analyzer>> aliases = new HashMap<String, Class<? extends Analyzer>>();

  private Set<String> _subIndexNames = new HashSet<String>();
  private Map<String, Set<String>> _subIndexNameLookups = new HashMap<String, Set<String>>();
  private Map<String, Boolean> _fullTextFields = new HashMap<String, Boolean>();
  private Map<String, Boolean> _fullTextColumnFamilies = new HashMap<String, Boolean>();
  private AnalyzerDefinition _analyzerDefinition;
  private Analyzer _defaultAnalyzer;
  private Analyzer _keywordAnalyzer = new KeywordAnalyzer();
  private Map<String, Analyzer> _analyzers = new HashMap<String, Analyzer>();
  private Map<String, TYPE> _typeLookup = new HashMap<String, BlurAnalyzer.TYPE>();
  private Map<String, FieldType> _fieldTypes = new HashMap<String, FieldType>();

  public Set<String> getSubIndexNames(String indexName) {
    return _subIndexNameLookups.get(indexName);
  }

  public BlurAnalyzer(Analyzer analyzer) {
    _analyzerDefinition = new AnalyzerDefinition();
    _defaultAnalyzer = analyzer;
  }

  public BlurAnalyzer(AnalyzerDefinition analyzerDefinition) {
    _analyzerDefinition = analyzerDefinition;
    ColumnDefinition defaultDefinition = analyzerDefinition.getDefaultDefinition();
    if (defaultDefinition == null) {
      defaultDefinition = new ColumnDefinition(STANDARD, true, null);
      analyzerDefinition.setDefaultDefinition(defaultDefinition);
    }
    _defaultAnalyzer = getAnalyzerByClassName(defaultDefinition.getAnalyzerClassName(), aliases, null, null,
        _fieldTypes);
    _analyzers = new HashMap<String, Analyzer>();
    _analyzers.put(ROW_ID, new KeywordAnalyzer());
    _analyzers.put(RECORD_ID, new KeywordAnalyzer());
    _analyzers.put(PRIME_DOC, new KeywordAnalyzer());
    _analyzers.put(FAMILY, new KeywordAnalyzer());
    _analyzers.put(SUPER, new NoStopWordStandardAnalyzer());
    load(_analyzers, _analyzerDefinition.columnFamilyDefinitions, _fullTextFields, _subIndexNameLookups,
        _subIndexNames, _fullTextColumnFamilies, _typeLookup, _fieldTypes);
  }

  public BlurAnalyzer() {
    this(new AnalyzerDefinition());
  }

  private Analyzer getAnalyzer(String name) {
    TYPE type = _typeLookup.get(name);
    if (type == TYPE.STRING) {
      return _keywordAnalyzer;
    } else if (type == TYPE.STORED) {
      throw new RuntimeException("Stored fields should never call this method.");
    }
    return _analyzers.get(name);
  }

  public TYPE getTypeLookup(String field) {
    TYPE type = _typeLookup.get(field);
    if (type == null) {
      return TYPE.TEXT;
    }
    return type;
  }

  public Query getNewRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) {
    TYPE type = _typeLookup.get(field);
    if (type == null) {
      return null;
    }
    FieldType fieldType = _fieldTypes.get(field);
    switch (type) {

    case STRING:
      return null;

    case INTEGER:
      int integerPrecisionStep = fieldType.numericPrecisionStep();
      int integerMin = Integer.parseInt(part1);
      int integerMax = Integer.parseInt(part2);
      return NumericRangeQuery.newIntRange(field, integerPrecisionStep, integerMin, integerMax, startInclusive,
          endInclusive);

    case DOUBLE:
      int doublePrecisionStep = fieldType.numericPrecisionStep();
      double doubleMin = Double.parseDouble(part1);
      double doubleMax = Double.parseDouble(part2);
      return NumericRangeQuery.newDoubleRange(field, doublePrecisionStep, doubleMin, doubleMax, startInclusive,
          endInclusive);

    case FLOAT:
      int floatPrecisionStep = fieldType.numericPrecisionStep();
      float floatMin = Float.parseFloat(part1);
      float floatMax = Float.parseFloat(part2);
      return NumericRangeQuery.newFloatRange(field, floatPrecisionStep, floatMin, floatMax, startInclusive,
          endInclusive);

    case LONG:
      int longPrecisionStep = fieldType.numericPrecisionStep();
      long longMin = Long.parseLong(part1);
      long longMax = Long.parseLong(part2);
      return NumericRangeQuery.newLongRange(field, longPrecisionStep, longMin, longMax, startInclusive, endInclusive);

    default:
      return null;
    }

  }

  public boolean isFullTextField(String fieldName) {
    Boolean b = _fullTextFields.get(fieldName);
    if (b != null) {
      return b;
    }
    String cf = getColumnFamily(fieldName);
    if (cf == null) {
      return false;
    }
    b = _fullTextColumnFamilies.get(cf);
    if (b != null) {
      return b;
    }
    ColumnDefinition defaultDefinition = _analyzerDefinition.getDefaultDefinition();
    if (defaultDefinition != null && defaultDefinition.fullTextIndex) {
      return true;
    }
    return false;
  }

  /**
   * This method decides on the field type for the given field by name. Sub
   * fields will also be passed in the fieldName such as fam1.col.sub1.
   * 
   * @param fieldName
   * @return the {@link FieldType}
   */
  public FieldType getFieldType(String field) {
    FieldType fieldType = _fieldTypes.get(field);
    if (fieldType == null) {
      fieldType = new FieldType(TextField.TYPE_STORED);
      fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      fieldType.setOmitNorms(true);
    }
    if (isSubFieldName(field)) {
      fieldType.setStored(false);
    }
    return fieldType;
  }

  /**
   * Checks if the fieldName is a sub field or not.
   * 
   * @param fieldName
   *          the field name to check.
   * @return boolean
   */
  public boolean isSubFieldName(String fieldName) {
    return _subIndexNames.contains(fieldName);
  }

  /**
   * Get field will return the proper field for the given {@link FieldType}.
   * 
   * @param fieldName
   *          the field name.
   * @param value
   *          the value.
   * @param fieldType
   *          the {@link FieldType}.
   * @return the new {@link Field}.
   */
  public Field getField(String fieldName, String value, FieldType fieldType) {
    TYPE type = _typeLookup.get(fieldName);
    if (type == null) {
      return new Field(fieldName, value, fieldType);
    }
    switch (type) {
    case STORED:
      return new StoredField(fieldName, value);
    case STRING:
      return new Field(fieldName, value, fieldType);
    case INTEGER:
      return new IntField(fieldName, Integer.parseInt(value), fieldType);
    case DOUBLE:
      return new DoubleField(fieldName, Double.parseDouble(value), fieldType);
    case FLOAT:
      return new FloatField(fieldName, Float.parseFloat(value), fieldType);
    case LONG:
      return new LongField(fieldName, Long.parseLong(value), fieldType);
    default:
      return new Field(fieldName, value, fieldType);
    }
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

  private String getColumnFamily(String fieldName) {
    int index = fieldName.indexOf('.');
    if (index < 0) {
      return null;
    }
    return fieldName.substring(0, index);
  }

  public AnalyzerDefinition getAnalyzerDefinition() {
    return _analyzerDefinition;
  }

  public void close() {

  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    Analyzer analyzer = getAnalyzer(fieldName);
    return (analyzer != null) ? analyzer : _defaultAnalyzer;
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return components;
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

  private static void load(Map<String, Analyzer> analyzers, Map<String, ColumnFamilyDefinition> familyDefinitions,
      Map<String, Boolean> fullTextFields, Map<String, Set<String>> subIndexNameLookups, Set<String> subIndexNames,
      Map<String, Boolean> fullTextColumnFamilies, Map<String, TYPE> typeLookup, Map<String, FieldType> fieldTypes) {
    if (familyDefinitions != null) {
      for (String family : familyDefinitions.keySet()) {
        ColumnFamilyDefinition familyDefinition = familyDefinitions.get(family);
        ColumnDefinition defaultDefinition = familyDefinition.getDefaultDefinition();
        if (defaultDefinition != null) {
          fullTextColumnFamilies.put(family, defaultDefinition.isFullTextIndex());
        }
        load(family, familyDefinition, analyzers, fullTextFields, subIndexNameLookups, subIndexNames, typeLookup,
            fieldTypes);
      }
    }
  }

  private static void load(String family, ColumnFamilyDefinition familyDefinition, Map<String, Analyzer> analyzers,
      Map<String, Boolean> fullTextFields, Map<String, Set<String>> subIndexNameLookups, Set<String> subIndexNames,
      Map<String, TYPE> typeLookup, Map<String, FieldType> fieldTypes) {
    Map<String, ColumnDefinition> columnDefinitions = familyDefinition.getColumnDefinitions();
    if (columnDefinitions != null) {
      for (String column : columnDefinitions.keySet()) {
        ColumnDefinition columnDefinition = columnDefinitions.get(column);
        load(family, familyDefinition, column, columnDefinition, analyzers, fullTextFields, subIndexNameLookups,
            subIndexNames, typeLookup, fieldTypes);
      }
    }
  }

  private static void load(String family, ColumnFamilyDefinition familyDefinition, String column,
      ColumnDefinition columnDefinition, Map<String, Analyzer> analyzers, Map<String, Boolean> fullTextFields,
      Map<String, Set<String>> subIndexNameLookups, Set<String> subIndexNames, Map<String, TYPE> typeLookup,
      Map<String, FieldType> fieldTypes) {
    Map<String, AlternateColumnDefinition> alternateColumnDefinitions = columnDefinition
        .getAlternateColumnDefinitions();
    if (alternateColumnDefinitions != null) {
      for (String subColumn : alternateColumnDefinitions.keySet()) {
        AlternateColumnDefinition alternateColumnDefinition = alternateColumnDefinitions.get(subColumn);
        load(family, familyDefinition, column, columnDefinition, subColumn, alternateColumnDefinition, analyzers,
            subIndexNameLookups, subIndexNames, typeLookup, fieldTypes);
      }
    }
    String fieldName = family + "." + column;
    Analyzer analyzer = getAnalyzerByClassName(columnDefinition.getAnalyzerClassName(), aliases, fieldName, typeLookup,
        fieldTypes);
    analyzers.put(fieldName, analyzer);
    if (columnDefinition.isFullTextIndex()) {
      fullTextFields.put(fieldName, Boolean.TRUE);
    } else {
      fullTextFields.put(fieldName, Boolean.FALSE);
    }
  }

  private static void load(String family, ColumnFamilyDefinition familyDefinition, String column,
      ColumnDefinition columnDefinition, String subColumn, AlternateColumnDefinition alternateColumnDefinition,
      Map<String, Analyzer> analyzers, Map<String, Set<String>> subIndexNameLookups, Set<String> subIndexNames,
      Map<String, TYPE> typeLookup, Map<String, FieldType> fieldTypes) {
    String fieldName = family + "." + column + "." + subColumn;
    Analyzer analyzer = getAnalyzerByClassName(alternateColumnDefinition.getAnalyzerClassName(), aliases, fieldName,
        typeLookup, fieldTypes);
    analyzers.put(fieldName, analyzer);
    addSubField(fieldName, subIndexNameLookups);
    subIndexNames.add(fieldName);
  }

  @SuppressWarnings("unchecked")
  private static Analyzer getAnalyzerByClassName(String className, Map<String, Class<? extends Analyzer>> aliases,
      String fieldName, Map<String, TYPE> typeLookup, Map<String, FieldType> fieldTypes) {
    TYPE type = getType(className, fieldName, fieldTypes);
    if (fieldName != null) {
      typeLookup.put(fieldName, type);
    }
    if (type != TYPE.TEXT) {
      return null;
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

  private static TYPE getType(String typeStr, String fieldName, Map<String, FieldType> fieldTypes) {
    if (typeStr == null) {
      return null;
    }
    String[] types = typeStr.split(",");
    String type = types[0];
    String typeUpper = type.toUpperCase();

    TYPE t = null;
    if (!typeNameCache.contains(typeUpper)) {
      t = TYPE.TEXT;
    } else {
      t = TYPE.valueOf(typeUpper);
    }

    FieldType fieldType;
    switch (t) {
    case STORED:
      fieldType = StoredField.TYPE;
      break;
    case STRING:
      fieldType = new FieldType(StringField.TYPE_STORED);
      break;
    case LONG:
      fieldType = new FieldType(LongField.TYPE_STORED);
      if (types.length > 1) {
        fieldType.setNumericPrecisionStep(Integer.parseInt(types[1]));
      }
      break;
    case INTEGER:
      fieldType = new FieldType(IntField.TYPE_STORED);
      if (types.length > 1) {
        fieldType.setNumericPrecisionStep(Integer.parseInt(types[1]));
      }
      break;
    case FLOAT:
      fieldType = new FieldType(FloatField.TYPE_STORED);
      if (types.length > 1) {
        fieldType.setNumericPrecisionStep(Integer.parseInt(types[1]));
      }
      break;
    case DOUBLE:
      fieldType = new FieldType(DoubleField.TYPE_STORED);
      if (types.length > 1) {
        fieldType.setNumericPrecisionStep(Integer.parseInt(types[1]));
      }
      break;
    default:
      fieldType = new FieldType(TextField.TYPE_STORED);
      fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
      fieldType.setOmitNorms(true);
      break;
    }
    fieldTypes.put(fieldName, fieldType);
    return t;
  }

  private static void addSubField(String name, Map<String, Set<String>> subIndexNameLookups) {
    int lastIndexOf = name.lastIndexOf('.');
    String mainFieldName = name.substring(0, lastIndexOf);
    Set<String> set = subIndexNameLookups.get(mainFieldName);
    if (set == null) {
      set = new TreeSet<String>();
      subIndexNameLookups.put(mainFieldName, set);
    }
    set.add(name);
  }

}
