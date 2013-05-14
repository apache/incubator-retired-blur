package org.apache.blur.mapreduce.csv;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.blur.mapreduce.BlurMapper;
import org.apache.blur.mapreduce.BlurMutate.MUTATE_TYPE;
import org.apache.blur.mapreduce.BlurRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.base.Splitter;

public class CsvBlurMapper extends BlurMapper<LongWritable, Text> {

  public static final String BLUR_CSV_FAMILY_COLUMN_PREFIX = "blur.csv.family.";
  public static final String BLUR_CSV_FAMILIES = "blur.csv.families";

  private Map<String, List<String>> columnNameMap;
  private String separator = ",";
  private Splitter splitter;

  public static void addColumns(Configuration configuration, String family, String... columns) {
    Collection<String> families = new TreeSet<String>(configuration.getStringCollection(BLUR_CSV_FAMILIES));
    families.add(family);
    configuration.setStrings(BLUR_CSV_FAMILIES, families.toArray(new String[] {}));
    configuration.setStrings(BLUR_CSV_FAMILY_COLUMN_PREFIX + family, columns);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration configuration = context.getConfiguration();
    Collection<String> familyNames = configuration.getStringCollection(BLUR_CSV_FAMILIES);
    columnNameMap = new HashMap<String, List<String>>();
    for (String family : familyNames) {
      String[] columnsNames = configuration.getStrings(BLUR_CSV_FAMILY_COLUMN_PREFIX + family);
      columnNameMap.put(family, Arrays.asList(columnsNames));
    }
    splitter = Splitter.on(separator);
  }

  @Override
  protected void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException {
    BlurRecord record = _mutate.getRecord();
    record.clearColumns();
    String str = value.toString();

    Iterable<String> split = splitter.split(str);
    List<String> list = toList(split);

    if (list.size() < 3) {
      throw new IOException("Record [" + str + "] too short.");
    }

    record.setRowId(list.get(0));
    record.setRecordId(list.get(1));
    String family = list.get(2);
    record.setFamily(family);

    List<String> columnNames = columnNameMap.get(family);
    if (columnNames == null) {
      throw new IOException("Family [" + family + "] is missing in the definition.");
    }
    if (list.size() - 3 != columnNames.size()) {
      throw new IOException("Record [" + str + "] too short, does not match defined record [rowid,recordid,family"
          + getColumnNames(columnNames) + "].");
    }

    for (int i = 0; i < columnNames.size(); i++) {
      record.addColumn(columnNames.get(i), list.get(i + 3));
      _fieldCounter.increment(1);
    }
    _key.set(record.getRowId());
    _mutate.setMutateType(MUTATE_TYPE.REPLACE);
    context.write(_key, _mutate);
    _recordCounter.increment(1);
    context.progress();
  }

  private String getColumnNames(List<String> columnNames) {
    StringBuilder builder = new StringBuilder();
    for (String c : columnNames) {
      builder.append(',').append(c);
    }
    return builder.toString();
  }

  private List<String> toList(Iterable<String> split) {
    List<String> lst = new ArrayList<String>();
    for (String s : split) {
      lst.add(s);
    }
    return lst;
  }

  public static void addColumns(Job job, String family, String columns) {
    addColumns(job.getConfiguration(), family, columns);
  }
}
