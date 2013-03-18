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
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

public class TermWritable implements Writable {

  private Term term;

  public TermWritable() {

  }

  public TermWritable(Term term) {
    this.term = term;
  }

  public Term getTerm() {
    return term;
  }

  public void setTerm(Term term) {
    this.term = term;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    String field = term.field();
    BytesRef bytes = term.bytes();
    SerializerUtil.writeString(field, out);
    SerializerUtil.writeBytesRef(bytes, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    String field = SerializerUtil.readString(in);
    BytesRef bytes = SerializerUtil.readBytesRef(in);
    term = new Term(field, bytes);
  }

}
