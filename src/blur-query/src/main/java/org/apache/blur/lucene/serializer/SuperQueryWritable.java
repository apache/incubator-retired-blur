package org.apache.blur.lucene.serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.blur.lucene.search.ScoreType;
import org.apache.blur.lucene.search.SuperQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

public class SuperQueryWritable extends AbtractQueryWritable<SuperQuery> {

  private SuperQuery query;

  @Override
  public void readFields(DataInput in) throws IOException {
    QueryWritable queryWritable = new QueryWritable();
    queryWritable.readFields(in);
    Query subQuery = queryWritable.getQuery();
    float boost = in.readFloat();
    TermWritable termWritable = new TermWritable();
    termWritable.readFields(in);
    Term primeDocTerm = termWritable.getTerm();
    String scoreType = IOUtil.readString(in);
    
    query = new SuperQuery(subQuery, ScoreType.valueOf(scoreType), primeDocTerm);
    query.setBoost(boost);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Query subQuery = query.getQuery();
    float boost = query.getBoost();
    Term primeDocTerm = query.getPrimeDocTerm();
    ScoreType scoreType = query.getScoreType();

    // Start writing
    new QueryWritable(subQuery).write(out);
    out.writeFloat(boost);
    new TermWritable(primeDocTerm).write(out);
    IOUtil.writeString(out, scoreType.name());
  }

  @Override
  public SuperQuery getQuery() {
    return query;
  }

  @Override
  public void setQuery(SuperQuery query) {
    this.query = query;
  }

  @Override
  public Class<SuperQuery> getType() {
    return SuperQuery.class;
  }

}
