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
  public void setup() {
    TableContext.clear();
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
      fieldManager.addColumnDefinition("fam", "col", null, false, "test", null);
      fail("should fail because new type is not loaded.");
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

    fieldManager.addColumnDefinition("fam", "col", null, false, "test", null);
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

    fieldManager.addColumnDefinition("fam", "col", null, false, "test", null);
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
