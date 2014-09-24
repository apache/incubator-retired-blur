package org.apache.blur.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.blur.utils.BlurUtil;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.util.BytesRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
@SuppressWarnings("serial")
public class TermsCommand extends Command implements ClusterReadCombiningCommand<BlurArray>,
    IndexReadCombiningCommand<BlurArray, BlurArray> {
  private static final String NAME = "terms";
  private static final String PARAMS = "params";
  private static final String P_SIZE = "size";
  private static final String P_FIELD = "fieldName";
  private static final String P_START = "startWith";

  private static final short DEFAULT_SIZE = 10;

  @Override
  public BlurArray execute(IndexContext context) throws IOException {
    BlurObject params = context.getArgs().get(PARAMS);
    short size = params.getShort(P_SIZE, DEFAULT_SIZE);
    String fieldName = params.get(P_FIELD);
    String startWith = params.getString(P_START, "");

    return new BlurArray(terms(context.getIndexReader(), fieldName, startWith, size));
  }

  @Override
  public BlurArray combine(CombiningContext context, Map<? extends Location<?>, BlurArray> results) throws IOException,
      InterruptedException {
    TreeSet<String> terms = Sets.newTreeSet();
    

    for (BlurArray t : results.values()) {
      terms.addAll((List<String>) t.asList());
    }

    short size = getSize(context);

    return new BlurArray(Lists.newArrayList(terms).subList(0, Math.min((int) size, terms.size())));
  }

  private short getSize(CombiningContext context) {
    short size = DEFAULT_SIZE;
    if ((context.getArgs() != null) && (context.getArgs().containsArg(PARAMS))) {
      BlurObject params = context.getArgs().get(PARAMS);
      size = params.getShort(P_SIZE, DEFAULT_SIZE);
    }
    return size;
  }

  @Override
  public String getName() {
    return NAME;
  }

  private static List<String> terms(IndexReader reader, String fieldName, String startWith, short size)
      throws IOException {

    Term term = getTerm(fieldName, startWith);
    List<String> terms = new ArrayList<String>(size);
    AtomicReader areader = BlurUtil.getAtomicReader(reader);
    Terms termsAll = areader.terms(term.field());

    if (termsAll == null) {
      return terms;
    }

    TermsEnum termEnum = termsAll.iterator(null);

    SeekStatus status = termEnum.seekCeil(term.bytes());

    if (status == SeekStatus.END) {
      return terms;
    }

    BytesRef currentTermText = termEnum.term();
    do {
      terms.add(currentTermText.utf8ToString());
      if (terms.size() >= size) {
        return terms;
      }
    } while ((currentTermText = termEnum.next()) != null);
    return terms;
  }

  private static Term getTerm(String fieldName, String value) {
    if (fieldName == null) {
      throw new NullPointerException("fieldName cannot be null.");
    }

    return new Term(fieldName, value);
  }

}
