package org.apache.blur.command;



import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class DocumentCountTest  {
  private static IndexContext ctx;
  
  @BeforeClass
  public static void init() {
    ctx = TestContext.newSimpleAlpaNumContext();
  }

  @Test
  public void documentCountShouldBeAccurate() throws IOException {
    DocumentCount dc = new DocumentCount();
    
    int docCount = dc.execute(ctx);
   
    assertEquals(26, docCount);
  }
  
}
