package ${groupId}.${artifactId};

import java.io.IOException;
import java.util.Map;

import org.apache.blur.command.annotation.Description;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.command.commandtype.ClusterServerReadCommandSingleTable;
import org.apache.lucene.index.Term;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

@Description("Returns the total number of occurrences of term across all documents (the sum of the freq() for each doc that has this term).")
public class MyCommand extends ClusterServerReadCommandSingleTable<Long> {
  private static final String NAME = "myCommand";

  @RequiredArgument
  private String fieldName;

  @RequiredArgument
  private String term;

  public TotalTermFreqCommand() {
    super();
  }

  public TotalTermFreqCommand(String fieldName, String term) {
    super();
    this.fieldName = fieldName;
    this.term = term;
  }

  @Override
  public Long execute(IndexContext context) throws IOException {
    return new Long(context.getIndexReader().totalTermFreq(new Term(fieldName, term)));
  }

  @Override
  public Long combine(CombiningContext context, Map<? extends Location<?>, Long> results) throws IOException,
      InterruptedException {

    Long total = 0l;

    for (Long shardTotal : results.values()) {
      total += shardTotal;
    }

    return total;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getTerm() {
    return term;
  }

  public void setValue(String term) {
    this.term = term;
  }
}
