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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

public class BooleanQueryWritable extends AbtractQueryWritable<BooleanQuery> {

  private BooleanQuery query;

  public BooleanQuery getQuery() {
    return query;
  }

  public void setQuery(BooleanQuery query) {
    this.query = query;
  }

  public BooleanQueryWritable() {

  }

  public BooleanQueryWritable(BooleanQuery booleanQuery) {
    this.query = booleanQuery;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(query.isCoordDisabled());
    out.writeFloat(query.getBoost());
    out.writeInt(query.getMinimumNumberShouldMatch());
    BooleanClause[] clauses = query.getClauses();
    out.writeInt(clauses.length);
    for (int i = 0; i < clauses.length; i++) {
      BooleanClauseWritable booleanClauseWritable = new BooleanClauseWritable(clauses[i]);
      booleanClauseWritable.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    query = new BooleanQuery(in.readBoolean());
    query.setBoost(in.readFloat());
    query.setMinimumNumberShouldMatch(in.readInt());
    int length = in.readInt();
    for (int i = 0; i < length; i++) {
      BooleanClauseWritable booleanClauseWritable = new BooleanClauseWritable();
      booleanClauseWritable.readFields(in);
      query.add(booleanClauseWritable.getBooleanClause());
    }

  }

  @Override
  public Class<BooleanQuery> getType() {
    return BooleanQuery.class;
  }

}
