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
package org.apache.blur.slur;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.blur.thrift.generated.BlurQuery;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class BlurQueryHelperTest {

  @Test
  public void simpleQueryString() {
    SolrParams p = new SolrQuery("foo");

    BlurQuery query = BlurQueryHelper.from(p);

    assertEquals("Should get our query string back.", "foo", query.getQuery().getQuery());
  }

  @Test(expected = IllegalArgumentException.class)
  public void fieldValuesMustFollowBlursFamilyColumnFormat() {
    SolrQuery p = new SolrQuery();

    p.setFields("foo");

    BlurQuery query = BlurQueryHelper.from(p);
  }

  @Test
  public void fieldsShouldTranslateToSelector() {
    SolrQuery p = new SolrQuery();

    p.setFields("fam1.col1", "fam1.col2", "fam2.col1");

    BlurQuery query = BlurQueryHelper.from(p);

    Map<String, Set<String>> columns = query.getSelector().getColumnsToFetch();

    assertTrue("Should have fam1 defined.", columns.containsKey("fam1"));
    assertTrue("Should have fam2 defined.", columns.containsKey("fam2"));

    Set<String> fam1 = columns.get("fam1");

    assertEquals("Should get all columns back.", 2, fam1.size());
    assertTrue("Should contain our column", fam1.contains("col1"));
    assertTrue("Should contain our column", fam1.contains("col2"));

    Set<String> fam2 = columns.get("fam2");
    assertEquals("Should get all columns back.", 1, fam2.size());
    assertTrue("Should contain our column", fam2.contains("col1"));

  }

}
