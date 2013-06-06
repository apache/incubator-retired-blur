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
import java.lang.reflect.Field;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;

public class FuzzyQueryWritable extends AbtractQueryWritable<FuzzyQuery> {

  private FuzzyQuery query;
  private static Field maxExpansionsField;
  private static Field transpositionsField;

  static {
    try {
      maxExpansionsField = FuzzyQuery.class.getDeclaredField("maxExpansions");
      transpositionsField = FuzzyQuery.class.getDeclaredField("transpositions");
      maxExpansionsField.setAccessible(true);
      transpositionsField.setAccessible(true);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  public FuzzyQueryWritable() {

  }

  public FuzzyQueryWritable(FuzzyQuery query) {
    this.query = query;
  }

  public FuzzyQuery getQuery() {
    return query;
  }

  public void setQuery(FuzzyQuery query) {
    this.query = query;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(query.getBoost());
    new TermWritable(query.getTerm()).write(out);
    out.writeInt(query.getMaxEdits());
    out.writeInt(query.getPrefixLength());
    out.writeInt(getMaxExpansions(query));
    out.writeBoolean(getTranspositions(query));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    float boost = in.readFloat();
    TermWritable termWritable = new TermWritable();
    termWritable.readFields(in);
    Term term = termWritable.getTerm();
    int maxEdits = in.readInt();
    int prefixLength = in.readInt();
    int maxExpansions = in.readInt();
    boolean transpositions = in.readBoolean();
    query = new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
    query.setBoost(boost);
  }

  private static boolean getTranspositions(FuzzyQuery query) {
    try {
      return transpositionsField.getBoolean(query);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static int getMaxExpansions(FuzzyQuery query) {
    try {
      return maxExpansionsField.getInt(query);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<FuzzyQuery> getType() {
    return FuzzyQuery.class;
  }
}
