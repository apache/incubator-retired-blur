package com.nearinfinity.blur.lucene.search;

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
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.Similarity;

public class FairSimilarity extends Similarity {

  private static final long serialVersionUID = 8819964136561756067L;

  @Override
  public float coord(int overlap, int maxOverlap) {
    return 1;
  }

  @Override
  public float idf(int docFreq, int numDocs) {
    return 1;
  }

  @Override
  public float queryNorm(float sumOfSquaredWeights) {
    return 1;
  }

  @Override
  public float sloppyFreq(int distance) {
    return 1;
  }

  @Override
  public float tf(float freq) {
    return 1;
  }

  @Override
  public float computeNorm(String field, FieldInvertState state) {
    return 1;
  }

}
