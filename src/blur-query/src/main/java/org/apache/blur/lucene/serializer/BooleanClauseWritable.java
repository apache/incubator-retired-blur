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

import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;

public class BooleanClauseWritable implements Writable {

  private BooleanClause booleanClause;

  public BooleanClauseWritable() {

  }

  public BooleanClauseWritable(BooleanClause booleanClause) {
    this.booleanClause = booleanClause;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Occur occur = booleanClause.getOccur();
    switch (occur) {
    case MUST:
      out.write(0);
      break;
    case MUST_NOT:
      out.write(1);
      break;
    case SHOULD:
      out.write(2);
      break;
    default:
      throw new RuntimeException("Occur [" + occur + "] not supported");
    }
    new QueryWritable(booleanClause.getQuery()).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Occur occur = null;
    byte o = in.readByte();
    switch (o) {
    case 0:
      occur = Occur.MUST;
      break;
    case 1:
      occur = Occur.MUST_NOT;
      break;
    case 2:
      occur = Occur.SHOULD;
      break;
    default:
      throw new RuntimeException("Occur [" + o + "] not supported");
    }
    QueryWritable queryWritable = new QueryWritable();
    queryWritable.readFields(in);
    booleanClause = new BooleanClause(queryWritable.getQuery(), occur);
  }

  public BooleanClause getBooleanClause() {
    return booleanClause;
  }

  public void setBooleanClause(BooleanClause booleanClause) {
    this.booleanClause = booleanClause;
  }
}
