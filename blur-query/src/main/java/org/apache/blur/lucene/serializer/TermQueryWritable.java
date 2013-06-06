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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

public class TermQueryWritable extends AbtractQueryWritable<TermQuery> {

  private TermQuery query;

  public TermQuery getQuery() {
    return query;
  }

  public void setQuery(TermQuery query) {
    this.query = query;
  }

  public TermQueryWritable() {

  }

  public TermQueryWritable(TermQuery termQuery) {
    this.query = termQuery;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(query.getBoost());
    Term term = query.getTerm();
    new TermWritable(term).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    float boost = in.readFloat();
    TermWritable termWritable = new TermWritable();
    termWritable.readFields(in);
    query = new TermQuery(termWritable.getTerm());
    query.setBoost(boost);
  }

  @Override
  public Class<TermQuery> getType() {
    return TermQuery.class;
  }

}
