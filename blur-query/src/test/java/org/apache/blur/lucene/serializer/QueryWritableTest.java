package org.apache.blur.lucene.serializer;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

public class QueryWritableTest {
  
  @Test
  public void testTermQuery() throws IOException {
    TermQuery query = new TermQuery(new Term("field","value"));
    QueryWritable queryWritable = new QueryWritable();
    queryWritable.setQuery(query);
    DataOutputBuffer out = new DataOutputBuffer();
    queryWritable.write(out);
    byte[] data = out.getData();
    int length = out.getLength();
    
    DataInputBuffer in = new DataInputBuffer();
    in.reset(data, length);
    
    QueryWritable newQueryWritable = new QueryWritable();
    newQueryWritable.readFields(in);
    
    Query termQuery = newQueryWritable.getQuery();
    
    assertEquals(query,termQuery);
    
  }

}
