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

import org.apache.blur.mapreduce.lib.CsvBlurMapper;
import org.apache.blur.mapreduce.lib.BlurMutate.MUTATE_TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class CsvBlurMapperTest {

  MapDriver<LongWritable, Text, Text, BlurMutate> mapDriver;

  @Before
  public void setUp() throws IOException {
    CsvBlurMapper mapper = new CsvBlurMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    Configuration configuration = mapDriver.getConfiguration();
    CsvBlurMapper.setColumns(configuration, "cf1:col1,col2|cf2:col1,col2,col3");
  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(), new Text("rowid1,record1,cf1,value1,value2"));
    mapDriver.withOutput(
        new Text("rowid1"),
        new BlurMutate(MUTATE_TYPE.REPLACE, "rowid1", "record1", "cf1").addColumn("col1", "value1").addColumn("col2",
            "value2"));
    mapDriver.runTest();
  }

}