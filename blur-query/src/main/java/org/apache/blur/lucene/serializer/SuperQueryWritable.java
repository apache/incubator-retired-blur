package org.apache.blur.lucene.serializer;

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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.blur.thrift.generated.ScoreType;
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
