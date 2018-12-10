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
package org.apache.blur.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.analysis.FieldManager;
import org.apache.blur.thrift.generated.TableDescriptor;
import org.apache.blur.utils.BlurConstants;
import org.junit.Before;
import org.junit.Test;

public class TableContextTest {

  private static final File TMPDIR = new File("./target/tmp");
  private File base;
  private String name = "testtable-TableContextTest";
  private File file;

  @Before
  public void setup() throws IOException {
    TableContext.clear();
    TableContext.setSystemBlurConfiguration(new BlurConfiguration());
    base = new File(TMPDIR, "TableContextTest");
    rm(base);
    file = new File(base, name);
    file.mkdirs();
  }

  @Test
  public void testLoadingNewTypeWhenNotDefined() throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(name);
    tableDescriptor.setTableUri(file.toURI().toString());
    TableContext context = TableContext.create(tableDescriptor);

    FieldManager fieldManager = context.getFieldManager();
    try {
      boolean result = fieldManager.addColumnDefinition("fam", "col", null, false, "test", false, false, null);
      fail("should fail because new type is not loaded [" + result + "].");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test
  public void testLoadingNewTypeWhenDefinedFromTableDescriptor() throws IOException {
    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(name);
    tableDescriptor.setTableUri(file.toURI().toString());
    tableDescriptor.putToTableProperties(BlurConstants.BLUR_FIELDTYPE + "test", TestType.class.getName());

    TableContext context = TableContext.create(tableDescriptor);
    FieldManager fieldManager = context.getFieldManager();

    fieldManager.addColumnDefinition("fam", "col", null, false, "test", false, false, null);
  }

  @Test
  public void testLoadingNewTypeWhenDefinedFromBlurConfigure() throws IOException {
    BlurConfiguration blurConfiguration = new BlurConfiguration();
    blurConfiguration.set(BlurConstants.BLUR_FIELDTYPE + "test", TestType.class.getName());

    TableContext.setSystemBlurConfiguration(blurConfiguration);

    TableDescriptor tableDescriptor = new TableDescriptor();
    tableDescriptor.setName(name);
    tableDescriptor.setTableUri(file.toURI().toString());

    TableContext context = TableContext.create(tableDescriptor);
    FieldManager fieldManager = context.getFieldManager();

    fieldManager.addColumnDefinition("fam", "col", null, false, "test", false, false, null);
  }

  private void rm(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

}
