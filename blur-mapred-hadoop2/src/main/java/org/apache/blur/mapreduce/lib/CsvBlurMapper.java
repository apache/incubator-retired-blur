package org.apache.blur.mapreduce.lib;

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
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Splitter;

/**
 * This will parse a standard csv file into a {@link BlurMutate} object. Use the
 * static addColumns, and setSeparator methods to configure the class.
 */
public class CsvBlurMapper extends BaseBlurMapper<Writable, Text> {

  public static final String UTF_8 = "UTF-8";
  public static final String BLUR_CSV_AUTO_GENERATE_RECORD_ID_AS_HASH_OF_DATA = "blur.csv.auto.generate.record.id.as.hash.of.data";
  public static final String BLUR_CSV_AUTO_GENERATE_ROW_ID_AS_HASH_OF_DATA = "blur.csv.auto.generate.row.id.as.hash.of.data";
  public static final String BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILIES = "blur.csv.family.path.mappings.families";
  public static final String BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILY_PREFIX = "blur.csv.family.path.mappings.family.";
  public static final String BLUR_CSV_SEPARATOR_BASE64 = "blur.csv.separator.base64";
  public static final String BLUR_CSV_FAMILY_COLUMN_PREFIX = "blur.csv.family.";
  public static final String BLUR_CSV_FAMILIES = "blur.csv.families";
  public static final String HIVE_NULL = "\\N";

  protected Map<String, List<String>> _columnNameMap;
  protected String _separator = Base64.encodeBase64String(",".getBytes());
  protected Splitter _splitter;
  protected boolean _familyNotInFile;
  protected String _familyFromPath;
  protected boolean _autoGenerateRecordIdAsHashOfData;
  protected MessageDigest _digest;
  protected boolean _autoGenerateRowIdAsHashOfData;

  /**
   * Add a mapping for a family to a path. This is to be used when an entire
   * path is to be processed as a single family and the data itself does not
   * contain the family.<br/>
   * <br/>
   * 
   * NOTE: the familyNotInFile property must be set before this method can be
   * called.
   * 
   * @param job
   *          the job to setup.
   * @param family
   *          the family.
   * @param path
   *          the path.
   */
  public static void addFamilyPath(Job job, String family, Path path) {
    addFamilyPath(job.getConfiguration(), family, path);
  }

  /**
   * Add a mapping for a family to a path. This is to be used when an entire
   * path is to be processed as a single family and the data itself does not
   * contain the family.<br/>
   * <br/>
   * 
   * NOTE: the familyNotInFile property must be set before this method can be
   * called.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param family
   *          the family.
   * @param path
   *          the path.
   */
  public static void addFamilyPath(Configuration configuration, String family, Path path) {
    append(configuration, BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILIES, family);
    append(configuration, BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILY_PREFIX + family, path.toString());
  }

  protected static void append(Configuration configuration, String name, String value) {
    Collection<String> set = configuration.getStringCollection(name);
    if (set == null) {
      set = new TreeSet<String>();
    }
    set.add(value);
    configuration.setStrings(name, set.toArray(new String[set.size()]));
  }

  /**
   * If set to true the record id will be automatically generated as a hash of
   * the data that the record contains.
   * 
   * @param job
   *          the job to setup.
   * @param autoGenerateRecordIdAsHashOfData
   *          boolean.
   */
  public static void setAutoGenerateRecordIdAsHashOfData(Job job, boolean autoGenerateRecordIdAsHashOfData) {
    setAutoGenerateRecordIdAsHashOfData(job.getConfiguration(), autoGenerateRecordIdAsHashOfData);
  }

  /**
   * If set to true the record id will be automatically generated as a hash of
   * the data that the record contains.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param autoGenerateRecordIdAsHashOfData
   *          boolean.
   */
  public static void setAutoGenerateRecordIdAsHashOfData(Configuration configuration,
      boolean autoGenerateRecordIdAsHashOfData) {
    configuration.setBoolean(BLUR_CSV_AUTO_GENERATE_RECORD_ID_AS_HASH_OF_DATA, autoGenerateRecordIdAsHashOfData);
  }

  /**
   * Gets whether or not to generate a recordid for the record based on the
   * data.
   * 
   * @param configuration
   *          the configuration.
   * @return boolean.
   */
  public static boolean isAutoGenerateRecordIdAsHashOfData(Configuration configuration) {
    return configuration.getBoolean(BLUR_CSV_AUTO_GENERATE_RECORD_ID_AS_HASH_OF_DATA, false);
  }

  /**
   * If set to true the record id will be automatically generated as a hash of
   * the data that the record contains.
   * 
   * @param job
   *          the job to setup.
   * @param autoGenerateRecordIdAsHashOfData
   *          boolean.
   */
  public static void setAutoGenerateRowIdAsHashOfData(Job job, boolean autoGenerateRowIdAsHashOfData) {
    setAutoGenerateRowIdAsHashOfData(job.getConfiguration(), autoGenerateRowIdAsHashOfData);
  }

  /**
   * If set to true the record id will be automatically generated as a hash of
   * the data that the record contains.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param autoGenerateRecordIdAsHashOfData
   *          boolean.
   */
  public static void setAutoGenerateRowIdAsHashOfData(Configuration configuration, boolean autoGenerateRowIdAsHashOfData) {
    configuration.setBoolean(BLUR_CSV_AUTO_GENERATE_ROW_ID_AS_HASH_OF_DATA, autoGenerateRowIdAsHashOfData);
  }

  /**
   * Gets whether or not to generate a recordid for the record based on the
   * data.
   * 
   * @param configuration
   *          the configuration.
   * @return boolean.
   */
  public static boolean isAutoGenerateRowIdAsHashOfData(Configuration configuration) {
    return configuration.getBoolean(BLUR_CSV_AUTO_GENERATE_ROW_ID_AS_HASH_OF_DATA, false);
  }

  /**
   * Sets all the family and column definitions.
   * 
   * @param job
   *          the job to setup.
   * @param strDefinition
   *          the string definition. <br/>
   * <br/>
   *          Example:<br/>
   *          "cf1:col1,col2,col3|cf2:col1,col2,col3"<br/>
   *          Where "cf1" is a family name that contains columns "col1", "col2"
   *          and "col3" and a second family of "cf2" with columns "col1",
   *          "col2", and "col3".
   */
  public static void setColumns(Job job, String strDefinition) {
    setColumns(job.getConfiguration(), strDefinition);
  }

  /**
   * Sets all the family and column definitions.
   * 
   * @param configuration
   *          the configuration to setup.
   * @param strDefinition
   *          the string definition. <br/>
   * <br/>
   *          Example:<br/>
   *          "cf1:col1,col2,col3|cf2:col1,col2,col3"<br/>
   *          Where "cf1" is a family name that contains columns "col1", "col2"
   *          and "col3" and a second family of "cf2" with columns "col1",
   *          "col2", and "col3".
   */
  public static void setColumns(Configuration configuration, String strDefinition) {
    Iterable<String> familyDefs = Splitter.on('|').split(strDefinition);
    for (String familyDef : familyDefs) {
      int indexOf = familyDef.indexOf(':');
      if (indexOf < 0) {
        throwMalformedDefinition(strDefinition);
      }
      String family = familyDef.substring(0, indexOf);
      Iterable<String> cols = Splitter.on(',').split(familyDef.substring(indexOf + 1));
      List<String> colnames = new ArrayList<String>();
      for (String columnName : cols) {
        colnames.add(columnName);
      }
      if (family.trim().isEmpty() || colnames.isEmpty()) {
        throwMalformedDefinition(strDefinition);
      }
      addColumns(configuration, family, colnames.toArray(new String[colnames.size()]));
    }
  }

  protected static void throwMalformedDefinition(String strDefinition) {
    throw new RuntimeException("Family and column definition string not valid [" + strDefinition
        + "] should look like \"family1:colname1,colname2|family2:colname1,colname2,colname3\"");
  }

  /**
   * Adds the column layout for the given family.
   * 
   * @param job
   *          the job to apply the layout.
   * @param family
   *          the family name.
   * @param columns
   *          the column names.
   */
  public static void addColumns(Job job, String family, String... columns) {
    addColumns(job.getConfiguration(), family, columns);
  }

  /**
   * Adds the column layout for the given family.
   * 
   * @param configuration
   *          the configuration to apply the layout.
   * @param family
   *          the family name.
   * @param columns
   *          the column names.
   */
  public static void addColumns(Configuration configuration, String family, String... columns) {
    Collection<String> families = new TreeSet<String>(configuration.getStringCollection(BLUR_CSV_FAMILIES));
    families.add(family);
    configuration.setStrings(BLUR_CSV_FAMILIES, families.toArray(new String[] {}));
    configuration.setStrings(BLUR_CSV_FAMILY_COLUMN_PREFIX + family, columns);
  }

  public static Collection<String> getFamilyNames(Configuration configuration) {
    return configuration.getStringCollection(BLUR_CSV_FAMILIES);
  }

  public static Map<String, List<String>> getFamilyAndColumnNameMap(Configuration configuration) {
    Map<String, List<String>> columnNameMap = new HashMap<String, List<String>>();
    for (String family : getFamilyNames(configuration)) {
      String[] columnsNames = configuration.getStrings(BLUR_CSV_FAMILY_COLUMN_PREFIX + family);
      columnNameMap.put(family, Arrays.asList(columnsNames));
    }
    return columnNameMap;
  }

  /**
   * Sets the separator of the file, by default it is ",".
   * 
   * @param job
   *          the job to apply the separator change.
   * @param separator
   *          the separator.
   */
  public static void setSeparator(Job job, String separator) {
    setSeparator(job.getConfiguration(), separator);
  }

  /**
   * Sets the separator of the file, by default it is ",".
   * 
   * @param configuration
   *          the configuration to apply the separator change.
   * @param separator
   *          the separator.
   */
  public static void setSeparator(Configuration configuration, String separator) {
    try {
      configuration.set(BLUR_CSV_SEPARATOR_BASE64, Base64.encodeBase64String(separator.getBytes(UTF_8)));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration configuration = context.getConfiguration();
    _autoGenerateRecordIdAsHashOfData = isAutoGenerateRecordIdAsHashOfData(configuration);
    _autoGenerateRowIdAsHashOfData = isAutoGenerateRowIdAsHashOfData(configuration);
    if (_autoGenerateRecordIdAsHashOfData || _autoGenerateRowIdAsHashOfData) {
      try {
        _digest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }
    _columnNameMap = getFamilyAndColumnNameMap(configuration);
    _separator = new String(Base64.decodeBase64(configuration.get(BLUR_CSV_SEPARATOR_BASE64, _separator)), UTF_8);
    _splitter = Splitter.on(_separator);
    Path fileCurrentlyProcessing = getCurrentFile(context);
    Collection<String> families = configuration.getStringCollection(BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILIES);
    OUTER: for (String family : families) {
      Collection<String> pathStrCollection = configuration
          .getStringCollection(BLUR_CSV_FAMILY_PATH_MAPPINGS_FAMILY_PREFIX + family);
      for (String pathStr : pathStrCollection) {
        Path path = new Path(pathStr);
        FileSystem fileSystem = path.getFileSystem(configuration);
        path = path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
        if (isParent(path, fileCurrentlyProcessing)) {
          _familyFromPath = family;
          _familyNotInFile = true;
          break OUTER;
        }
      }
    }
  }

  protected boolean isParent(Path possibleParent, Path child) {
    if (child == null) {
      return false;
    }
    if (possibleParent.equals(child.getParent())) {
      return true;
    }
    return isParent(possibleParent, child.getParent());
  }

  protected Path getCurrentFile(Context context) throws IOException {
    InputSplit split = context.getInputSplit();
    if (split != null && split instanceof FileSplit) {
      FileSplit inputSplit = (FileSplit) split;
      Path path = inputSplit.getPath();
      FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
      return path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    }
    return null;
  }

  @Override
  protected void map(Writable k, Text value, Context context) throws IOException, InterruptedException {
    BlurRecord record = _mutate.getRecord();
    record.clearColumns();
    String str = value.toString();

    Iterable<String> split = _splitter.split(str);
    List<String> list = toList(split);

    int offset = 0;
    boolean gen = false;
    if (!_autoGenerateRowIdAsHashOfData) {
      record.setRowId(list.get(offset++));
    } else {
      _digest.reset();
      byte[] bs = value.getBytes();
      int length = value.getLength();
      _digest.update(bs, 0, length);
      record.setRowId(new BigInteger(_digest.digest()).toString(Character.MAX_RADIX));
      gen = true;
    }

    if (!_autoGenerateRecordIdAsHashOfData) {
      record.setRecordId(list.get(offset++));
    } else {
      if (gen) {
        record.setRecordId(record.getRowId());
      } else {
        _digest.reset();
        byte[] bs = value.getBytes();
        int length = value.getLength();
        _digest.update(bs, 0, length);
        record.setRecordId(new BigInteger(_digest.digest()).toString(Character.MAX_RADIX));
      }
    }
    String family;
    if (_familyNotInFile) {
      family = _familyFromPath;
    } else {
      family = list.get(offset++);
    }
    record.setFamily(family);

    List<String> columnNames = _columnNameMap.get(family);
    if (columnNames == null) {
      throw new IOException("Family [" + family + "] is missing in the definition.");
    }
    if (list.size() - offset != columnNames.size()) {

      String options = "";

      if (!_autoGenerateRowIdAsHashOfData) {
        options += "rowid,";
      }
      if (!_autoGenerateRecordIdAsHashOfData) {
        options += "recordid,";
      }
      if (!_familyNotInFile) {
        options += "family,";
      }
      String msg = "Record [" + str + "] does not match defined record [" + options + getColumnNames(columnNames)
          + "].";
      throw new IOException(msg);
    }

    for (int i = 0; i < columnNames.size(); i++) {
      String val = handleHiveNulls(list.get(i + offset));
      if (val != null) {
        record.addColumn(columnNames.get(i), val);
        _columnCounter.increment(1);
      }
    }
    _key.set(record.getRowId());
    _mutate.setMutateType(MUTATE_TYPE.REPLACE);
    context.write(_key, _mutate);
    _recordCounter.increment(1);
    context.progress();
  }

  protected String handleHiveNulls(String value) {
    if (value.equals(HIVE_NULL)) {
      return null;
    }
    return value;
  }

  public void setFamilyFromPath(String familyFromPath) {
    this._familyFromPath = familyFromPath;
  }

  protected String getColumnNames(List<String> columnNames) {
    StringBuilder builder = new StringBuilder();
    for (String c : columnNames) {
      if (builder.length() != 0) {
        builder.append(',');
      }
      builder.append(c);
    }
    return builder.toString();
  }

  protected List<String> toList(Iterable<String> split) {
    List<String> lst = new ArrayList<String>();
    for (String s : split) {
      lst.add(s);
    }
    return lst;
  }

}
