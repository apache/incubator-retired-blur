package org.apache.blur.lucene.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.lucene.search.MatchAllDocsQuery;

public class MatchAllDocsQueryWritable extends AbtractQueryWritable<MatchAllDocsQuery> {

  private MatchAllDocsQuery query;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(query.getBoost());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    float boost = in.readFloat();
    query = new MatchAllDocsQuery();
    query.setBoost(boost);
  }

  @Override
  public MatchAllDocsQuery getQuery() {
    return query;
  }

  @Override
  public void setQuery(MatchAllDocsQuery query) {
    this.query = query;
  }

  @Override
  public Class<MatchAllDocsQuery> getType() {
    return MatchAllDocsQuery.class;
  }

}
