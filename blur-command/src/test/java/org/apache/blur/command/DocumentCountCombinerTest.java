package org.apache.blur.command;



import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class DocumentCountCombinerTest  {
  private static IndexContext ctx;
  
  @BeforeClass
  public static void init() {
    ctx = TestContext.newSimpleAlpaNumContext();
  }

  @Test
  public void documentCountShouldBeAccurate() throws IOException {
    DocumentCountCombiner dc = new DocumentCountCombiner();
    
    int docCount = dc.execute(ctx);
   
    assertEquals(26, docCount);
  }
  
  @Test 
  public void combineShouldProperlySum() throws IOException {
    DocumentCountCombiner dc = new DocumentCountCombiner();
    Map<Shard, Integer>  shardTotals = Maps
        .newHashMap(ImmutableMap
            .of(new Shard("t1","s1"), 10, new Shard("t1","s2"), 20, new Shard("t1","s3"), 30));
    long total = dc.combine(shardTotals);
    
    assertEquals(60l, total);
  }
  
}
